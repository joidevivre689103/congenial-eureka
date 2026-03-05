#!/usr/bin/env python3
"""
Extract enriched data.js from binary .dat files + existing data.js
Adds match year, result with margin, series name, dates, host to match data.
Preserves all existing data.js structure.
"""

import struct
import json
import os

# === Configuration ===
PLAYER_DAT = '/mnt/user-data/uploads/testply__1_.dat'
MATCH_DAT = '/mnt/user-data/uploads/testmat__1_.dat'
EXISTING_DATA_JS = '/mnt/user-data/uploads/data.js'
OUTPUT_FILE = '/mnt/user-data/outputs/data.js'

PLAYER_RECORD_SIZE = 8000
MATCH_RECORD_SIZE = 10000
NUM_MATCHES = 2616
NUM_PLAYER_SLOTS = 3281


def read_str(data, offset, max_len=50):
    """Read null-terminated string from binary data."""
    end = data.find(b'\x00', offset, offset + max_len)
    if end == -1:
        end = offset + max_len
    return data[offset:end].decode('ascii', errors='replace').strip()


def load_existing_data_js(path):
    """Load the existing data.js and parse the D object."""
    with open(path, 'r') as f:
        content = f.read()
    json_str = content.replace('const D = ', '', 1).rstrip().rstrip(';')
    return json.loads(json_str)


def extract_match_data(dat_path):
    """Extract enriched match data from testmat.dat."""
    matches = {}
    with open(dat_path, 'rb') as f:
        for i in range(NUM_MATCHES):
            f.seek(i * MATCH_RECORD_SIZE)
            rec = f.read(MATCH_RECORD_SIZE)
            
            match_id = i + 1
            
            # String fields
            series = read_str(rec, 0, 44)
            match_num = read_str(rec, 44, 22)
            host = read_str(rec, 66, 14)
            dates = read_str(rec, 80, 44)
            ground = read_str(rec, 124, 75)
            result = read_str(rec, 200, 75)
            umpire1 = read_str(rec, 286, 34)
            umpire2 = read_str(rec, 320, 34)
            city = read_str(rec, 365, 20)
            
            # Numeric fields
            year = struct.unpack_from('<H', rec, 0x1C2)[0]
            year_end = struct.unpack_from('<H', rec, 0x1C4)[0]
            match_no = struct.unpack_from('<H', rec, 0x1C6)[0]
            total_runs = struct.unpack_from('<H', rec, 0x1E0)[0]
            total_wickets = struct.unpack_from('<H', rec, 0x1E2)[0]
            
            matches[str(match_id)] = {
                'series': series,
                'match_num': match_num,
                'host': host,
                'dates': dates,
                'ground': ground,
                'result': result,
                'umpire1': umpire1,
                'umpire2': umpire2,
                'city': city,
                'year': year,
                'year_end': year_end,
                'total_runs': total_runs,
                'total_wickets': total_wickets,
            }
    
    return matches


def build_enriched_data(existing, dat_matches):
    """
    Build the enriched D object.
    
    Existing M[id] = [cityIdx, groundIdx, team1Idx, team2Idx, winnerIdx, tossIdx, homeAwayCode]
    
    New M[id] = [cityIdx, groundIdx, team1Idx, team2Idx, winnerIdx, tossIdx, homeAwayCode,
                 year, resultString, series, dates, matchNum, totalRuns, totalWickets]
    
    To keep the file compact, we'll store the new string fields in lookup arrays
    and reference them by index, just like the existing approach.
    """
    
    # Build lookup arrays for new string fields
    result_strings = []
    result_map = {}
    series_strings = []
    series_map = {}
    date_strings = []
    date_map = {}
    match_num_strings = []
    match_num_map = {}
    
    def get_or_add(value, lst, mapping):
        if value not in mapping:
            mapping[value] = len(lst)
            lst.append(value)
        return mapping[value]
    
    # Enrich match data
    enriched_M = {}
    for match_id_str, existing_m in existing['M'].items():
        dat_match = dat_matches.get(match_id_str, None)
        
        if dat_match:
            result_idx = get_or_add(dat_match['result'], result_strings, result_map)
            series_idx = get_or_add(dat_match['series'], series_strings, series_map)
            dates_idx = get_or_add(dat_match['dates'], date_strings, date_map)
            match_num_idx = get_or_add(dat_match['match_num'], match_num_strings, match_num_map)
            
            # Extend existing 7-field array with new fields
            enriched_M[match_id_str] = existing_m + [
                dat_match['year'],       # index 7: year
                result_idx,              # index 8: result string index -> R array
                series_idx,              # index 9: series string index -> SR array
                dates_idx,               # index 10: dates string index -> DT array
                match_num_idx,           # index 11: match num string index -> MN array
                dat_match['total_runs'], # index 12: total runs
                dat_match['total_wickets']  # index 13: total wickets
            ]
        else:
            # Keep original if no dat match found
            enriched_M[match_id_str] = existing_m
    
    # Build the enriched D object
    enriched = {
        'T': existing['T'],      # Teams
        'C': existing['C'],      # Cities
        'G': existing['G'],      # Grounds
        'L': existing['L'],      # Locations
        'R': result_strings,     # NEW: Result strings (with margins)
        'SR': series_strings,    # NEW: Series names
        'DT': date_strings,      # NEW: Match dates
        'MN': match_num_strings, # NEW: Match numbers ("1st Test", etc.)
        'M': enriched_M,         # ENRICHED: Match data
        'PT': existing['PT'],    # Player -> Team mapping
        'S': existing['S'],      # Scorecard entries
        'P': existing['P'],      # Player career data
    }
    
    return enriched


def write_data_js(data, output_path):
    """Write the enriched data as a compact JS file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Use compact JSON (no spaces) for minimum file size
    json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=True)
    
    with open(output_path, 'w') as f:
        f.write(f'const D = {json_str};')
    
    return os.path.getsize(output_path)


def print_summary(existing, enriched, dat_matches, file_size):
    """Print a summary of what was extracted and enriched."""
    print("=" * 65)
    print("  DATA EXTRACTION & ENRICHMENT COMPLETE")
    print("=" * 65)
    
    print(f"\n  Source files:")
    print(f"    testmat.dat:  {os.path.getsize(MATCH_DAT):>12,} bytes")
    print(f"    testply.dat:  {os.path.getsize(PLAYER_DAT):>12,} bytes")
    print(f"    data.js (old):{os.path.getsize(EXISTING_DATA_JS):>12,} bytes")
    
    print(f"\n  Output:")
    print(f"    data.js (new):{file_size:>12,} bytes")
    
    old_m_fields = len(next(iter(existing['M'].values())))
    new_m_fields = len(next(iter(enriched['M'].values())))
    
    print(f"\n  Data structure (D object keys):")
    print(f"    {'Key':<6} {'Description':<30} {'Count':>8} {'Status'}")
    print(f"    {'─'*6} {'─'*30} {'─'*8} {'─'*10}")
    print(f"    {'T':<6} {'Teams':<30} {len(enriched['T']):>8} preserved")
    print(f"    {'C':<6} {'Cities':<30} {len(enriched['C']):>8} preserved")
    print(f"    {'G':<6} {'Grounds':<30} {len(enriched['G']):>8} preserved")
    print(f"    {'L':<6} {'Locations':<30} {len(enriched['L']):>8} preserved")
    print(f"    {'R':<6} {'Result strings':<30} {len(enriched['R']):>8} NEW")
    print(f"    {'SR':<6} {'Series names':<30} {len(enriched['SR']):>8} NEW")
    print(f"    {'DT':<6} {'Match dates':<30} {len(enriched['DT']):>8} NEW")
    print(f"    {'MN':<6} {'Match numbers':<30} {len(enriched['MN']):>8} NEW")
    m_desc = f'Matches ({old_m_fields}→{new_m_fields} fields)'
    print(f"    {'M':<6} {m_desc:<30} {len(enriched['M']):>8} ENRICHED")
    print(f"    {'PT':<6} {'Player→Team map':<30} {len(enriched['PT']):>8} preserved")
    print(f"    {'S':<6} {'Scorecard entries':<30} {len(enriched['S']):>8} preserved")
    print(f"    {'P':<6} {'Players (32 fields each)':<30} {len(enriched['P']):>8} preserved")
    
    print(f"\n  New M[id] field layout:")
    print(f"    [0] cityIdx      [1] groundIdx    [2] team1Idx")
    print(f"    [3] team2Idx     [4] winnerIdx    [5] tossWinnerIdx")
    print(f"    [6] homeAwayCode [7] year         [8] resultIdx → R[]")
    print(f"    [9] seriesIdx → SR[]              [10] datesIdx → DT[]")
    print(f"    [11] matchNumIdx → MN[]           [12] totalRuns")
    print(f"    [13] totalWickets")
    
    # Show a sample enriched match
    sample_id = '1'
    m = enriched['M'][sample_id]
    print(f"\n  Sample: M[{sample_id}] = {m}")
    print(f"    → {enriched['C'][m[0]]}, {enriched['G'][m[1]]}")
    print(f"    → {enriched['T'][m[2]]} vs {enriched['T'][m[3]]}")
    print(f"    → Year: {m[7]}, Result: \"{enriched['R'][m[8]]}\"")
    print(f"    → Series: \"{enriched['SR'][m[9]]}\"")
    print(f"    → Dates: \"{enriched['DT'][m[10]]}\"")
    print(f"    → Match: \"{enriched['MN'][m[11]]}\"")
    print(f"    → Total runs: {m[12]}, wickets: {m[13]}")
    
    print(f"\n  Size comparison:")
    old_size = os.path.getsize(EXISTING_DATA_JS)
    delta = file_size - old_size
    pct = (delta / old_size) * 100
    print(f"    Old: {old_size:>10,} bytes")
    print(f"    New: {file_size:>10,} bytes")
    print(f"    Delta: {delta:>+9,} bytes ({pct:+.1f}%)")
    print()


def main():
    print("Loading existing data.js...")
    existing = load_existing_data_js(EXISTING_DATA_JS)
    
    print(f"Extracting match data from {NUM_MATCHES} records in testmat.dat...")
    dat_matches = extract_match_data(MATCH_DAT)
    
    print("Building enriched data structure...")
    enriched = build_enriched_data(existing, dat_matches)
    
    print(f"Writing enriched data.js...")
    file_size = write_data_js(enriched, OUTPUT_FILE)
    
    print_summary(existing, enriched, dat_matches, file_size)


if __name__ == '__main__':
    main()

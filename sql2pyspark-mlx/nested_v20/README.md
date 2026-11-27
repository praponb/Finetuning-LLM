# nested_v20 — free template pairs for the harness gaps (q9g round, 2026-11-26)

Harness v2 (`eval/harness_suite_v2.jsonl`, **test only**) showed the Qwen3.5 models failing on nested types, maps, JSON,
higher-order functions, generators, semi / anti joins, set operations, interval window frames, time buckets, gap
filling, DECIMAL typing and NULL semantics. The owner chose not to spend more on the Kimi API, so this folder builds
**training** pairs for those forms from templates, free of charge.

- `generate.py` — 17 families: `posexplode`, `inline`, `map`, `json`, `hof`, `array_fns`, `by_agg`, `semi_anti`, `setop`,
  `range_frame`, `unpivot`, `time_bucket`, `spine`, `decimal`, `group_all_tuple`, `struct_collect` and a 3-stage
  `pipeline` (explode → per-id aggregate with `max_by` → semi / anti filter → ranking window). Every pair gets its own
  small typed tables (`SELECT CAST(..) .. FROM VALUES`: `ARRAY<STRUCT>`, `STRUCT`, `MAP<STRING,INT>`, a JSON string,
  `ARRAY<INT>`, `DECIMAL(12,2)`, `DATE`, `TIMESTAMP`, NULLs), with names from the **30 training domains** of
  `schema_v15/synth_catalog.py` (never the 5 held-out domains, never the 13 suite tables). SQL and PySpark are written
  side by side; ~half the SQL is lowercase.
- `verify.py` — keeps a pair only if `sql2pyspark_harness/compare_harness.compare_one` passes (same column names **and**
  Spark types, same row multiset, every value computed) and `verify_pairs.compare` passes with the `ORDER BY` columns.
  Worker processes with a local Spark session each.

```bash
cd sql2pyspark-mlx
.venv/bin/python nested_v20/generate.py --n 6000 --seed 20 --out nested_v20/raw/cands.jsonl
.venv/bin/python nested_v20/verify.py nested_v20/raw/cands.jsonl nested_v20/raw/verified.jsonl --workers 3
```

## Run (2026-11-26)

| | |
| --- | --- |
| Candidates / kept | 6,000 / **5,797** (96.6%; the rest = result row count outside 2-60 or an all-NULL column; **0** mismatches) |
| Time | ~25 min (3 workers) |
| Distinct SQL | 5,720 |
| Used in `data_q9g` | 4,000 train (evened over the families) + 100 valid; leakage 0 vs 2,195 eval SQL strings; spot re-check 200/200 |

Outputs in `raw/` are git-ignored. The mix is built by `qwen35_pilot/polishmix/build_q9g.py`.

# complex_v16 — 10,000 complex pairs on the synthetic schemas (v16)

Training data for **v16**, which continues from **v15** ([schema_v15](../schema_v15/README.md)).

## Why

The complex forms (multi-stage pipelines, many-window queries, set operations, gaps-and-islands, grouping sets, pivot /
unpivot) were last trained in v8 / v9 on complex_v8's 20 tables, the same tables as the complex test set. v15 showed
that new names, not more pairs on known tables, are what generalizes to unseen schemas. These 10,000 pairs therefore
put the complex forms on the 30 synthetic training domains of schema_v15, and add longer pipelines (6–9 stages)
towards the long real-world queries.

## What is in it

[`cx_patch.py`](cx_patch.py) converts the synthetic tables to complex_v8 `Col` / `Table` and installs them into
`complex_v8/catalog.py` before [`../complex_v8/generate.py`](../complex_v8/generate.py) is imported (and in every
spawned verify worker); `rebind()` replaces the generator's hard-coded table lists (`FACT_TABLES`, `ALL_TABLES`, the
FULL OUTER splits) with synthetic ones. [`generate.py`](generate.py) then runs the complex families in the mirror window
style:

| Family | Weight | Content |
| --- | --- | --- |
| `cx_pipeline` | 30 | 3–7 stages: joins, EXISTS / NOT EXISTS / IN, correlated scalar subqueries, dedup, compare-to-group, explode, aggregation |
| `cx_long_pipeline` | 10 | the same with 6–9 stages |
| `cx_window_heavy` | 18 | 8–16 window functions over 3–6 specs, named `WINDOW` clauses, frames, `IGNORE NULLS`, `NTH_VALUE`, `NTILE` |
| `cx_setop` | 14 | `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]`, positional unions of differently named columns |
| `cx_sessionize` | 8 | gaps-and-islands on a synthetic table with a timestamp and a key |
| `cx_full_outer` | 5 | two aggregates FULL OUTER JOINed with COALESCE keys |
| `cx_grouping` | 10 | ROLLUP / CUBE / GROUPING SETS with GROUPING_ID |
| `cx_reshape` | 8 | PIVOT / UNPIVOT |

`cx_interval` is left out (it needs complex_v8's own coverage / tariff / ticket tables). Pairs over 3,000 tokens,
empty results and SQL from an eval set are dropped. **Complex schema-shift test set** (`data/complex_shift_meta.jsonl`,
300 pairs): the same families on the 5 schema_v15 `HELDOUT_DOMAINS`; evaluate with `--tables eval/complex_shift_tables.py`.

| Path | Content |
| --- | --- |
| `cx_patch.py`, `generate.py`, `verify_v16.py` | catalog patch, generator, `../complex_v8/verify.py` with the patch in every worker |
| `build.py` | `data/` (train / valid / `complex_shift_meta.jsonl`) and `data_mix/` |
| `data_mix/` | what v16 trains on: 10,000 new + 2,500 schema_v15 + 1,500 targeted_v14 + all 708 classic scalar-subquery pairs + 3,300 classic + 1,500 complex_v9 + 800 targeted_v12 + 700 targeted_v11 |
| `lora_config.yaml` | continues from v15 (`../schema_v15/adapters`), 3,600 micro-batches, writes `complex_v16/adapters/` |

## Rebuild

```bash
cd sql2pyspark-mlx/complex_v16
../.venv/bin/python generate.py --split train --out raw_v16            # 13,500 raw pairs
../.venv/bin/python generate.py --split heldout --out raw_v16_shift    # 450 raw pairs
../.venv/bin/python verify_v16.py --src raw_v16 --workers 5 2>/dev/null
../.venv/bin/python verify_v16.py --src raw_v16_shift --workers 5 2>/dev/null
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v16.sh` from the repo root.

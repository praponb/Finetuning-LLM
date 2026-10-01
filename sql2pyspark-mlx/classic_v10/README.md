# classic_v10 — the classic pairs in the "mirror" window style + IN / NOT IN without SQL aliases

Training data for **v10**, the follow-up to v9 ([complex_v9](../complex_v9/README.md)).

## Why

* **`real001` still loops on v9.** v9 learned the mirror window style only on the complex_v8 tables; the classic v1–v7
  training targets still hoist every window spec as `w`, `w2`, … `w21` at the top of the script (6,829 of the 16,923
  v7 training pairs, up to 21 variables each), and that is exactly the block v9 repeats on the real queries
  (`w70`, `w71`, …).
* **`holdout024` / `holdout052`.** Every IN-subquery in the classic data aliases the outer table in the SQL
  (`FROM orders o WHERE o.customer_id IN (...)`), and `id NOT IN (SELECT fk FROM child)` never appears. On the bare
  form `SELECT id, doctor_name FROM doctors WHERE id NOT IN (SELECT doctor_id FROM appointments ...)` v8/v9 alias the
  DataFrames but write an unqualified `F.col("id")` in the join condition (ambiguous).

## What is in it

| Path | Content |
|---|---|
| `mirror.py` | rewrites hoisted window code to the mirror style: each window variable is substituted where it is used (derived ones like `w2 = w.rowsBetween(...)` resolved first); a variable stays only when the SQL names the window (`WINDOW w AS (...)`) |
| `inlist.py` | 900 targeted pairs: `parent.id [NOT] IN (SELECT child.fk ...)` and `child.fk [NOT] IN (SELECT parent.id ...)` with no SQL aliases; the PySpark side aliases both DataFrames and qualifies the join columns; NOT IN over a nullable column gets an explicit `IS NOT NULL` |
| `build.py` | `prepare` (mirror the v7 pairs) and `split` (data/ + data_mix/) |
| `data/` | the v7 split (same train / valid / test membership by SQL) with mirrored code; IN / NOT IN pairs in train (817) and `inlist_valid_meta.jsonl` (50 held out for evaluation) |
| `data_mix/` | what v10 trains on (12,463): 6,646 classic pairs whose window code changed + 817 IN / NOT IN pairs + 2,000 classic pairs without windows + 3,000 complex_v9 pairs (already mirror style) |
| `lora_config.yaml` | continues from v9 (`../complex_v9/adapters`), writes `classic_v10/adapters/` |

Spark verification: 7,857 of the 7,858 rewritten classic pairs still match their SQL (the one failure is dropped), and
900 of 900 IN / NOT IN pairs match (867 with a non-empty result are used).

## Rebuild

```bash
cd sql2pyspark-mlx/classic_v10
../.venv/bin/python build.py prepare                                   # ../raw_pairs/verified.jsonl -> raw_pairs/
../.venv/bin/python inlist.py --n 900 --seed 10 --out raw_inlist
../.venv/bin/python ../verify_pairs.py --src raw_pairs --workers 5 2>/dev/null     # ~15 min
../.venv/bin/python ../verify_pairs.py --src raw_inlist --workers 5 2>/dev/null    # ~1.5 min
../.venv/bin/python build.py split
```

Train, export and evaluate: `zsh eval/start_training_v10.sh` from the repo root.

## Results (v10, 2026-10-01)

Trained 2026-10-01 12:18 → 18:42 (6 h 24 min, peak 9.9 GB), validation loss 0.013 → 0.012 → 0.006 → **0.007**
(`train_v10.log`). v10-q4 (Q4_K_M) vs. v9-q4 (`latest`):

| Eval set | v9-q4 | v10-q4 |
|---|---|---|
| Held-out `IN` / `NOT IN` (50, `data/inlist_valid_meta.jsonl`) | 11 | **49** |
| Hold-out (62) | 59 | **60** |
| Fixed test set (150) | 140 | **147** |
| v4 hold-out (25) | **21** | 18 |
| complex_v8 test (100) | **87** | 85 |
| Real-world queries (4) | 0 | 0 (`real001` no longer loops) |

Both targets were hit: bare `IN` / `NOT IN` now qualify the join columns, and `real001` writes its windows inline and
ends normally. One new slip comes from the `IN` / `NOT IN` pattern: a correlated `NOT EXISTS` with an outer column in a
non-equality condition (`holdout_v4_012`) gets that condition in the inner `.filter()` instead of the join condition.
Details and the promotion recommendation: [README → v10 results](../../README.md#v10-results-2026-10-01).

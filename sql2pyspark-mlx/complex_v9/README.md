# complex_v9 — the complex_v8 pairs with "mirror" window code (+ 1,990 window-heavy pairs)

Training data for **v9**. The SQL side, the tables and the train / valid / test split are those of
[complex_v8](../complex_v8/README.md); only the PySpark side of queries that use windows changed.

## Why

v8 scored 1/15 on the many-window family (`cx_window_heavy`) of the complex_v8 test set although it had 1,690 training
pairs of that family. Its answers show the cause: the v8 target code defines **every** window spec as a variable at the
top of the script, so the model has to plan 8-16 near-identical lines before it writes the first expression. It then
repeats a definition until it runs out of tokens (`w_..._2`, `w_..._3`, ... `w_..._56`), uses a variable it never
defined, or attaches the wrong frame to a name. The same loop breaks `real001` (`w1` ... `w93`).

## The "mirror" style

The PySpark code now follows the SQL one to one:

| SQL | PySpark (v8, "hoist") | PySpark (v9, "mirror") |
|---|---|---|
| `RANK() OVER (PARTITION BY a ORDER BY b DESC)` | `w_a_by_b = Window...` at the top, `.over(w_a_by_b)` | `F.rank().over(Window.partitionBy("a").orderBy(F.col("b").desc()))` |
| `SUM(x) OVER w_a` + `WINDOW w_a AS (PARTITION BY a)` | `w_a = Window...` at the top, `.over(w_a)` | `w_a = Window.partitionBy("a")` right before the stage that uses it, `.over(w_a)` |

So there is no definitions block to plan, frames are written where the SQL has them, and a variable exists only when
the SQL names the window. `../complex_v8/generate.py --window-style mirror` renders this (default `hoist` reproduces
complex_v8 byte for byte); the SQL is identical in both styles.

## What is in it

| Path | Content |
|---|---|
| `data/train.jsonl` | 11,989 pairs: the complex_v8 training pairs re-rendered (9,999) + 1,990 extra `cx_window_heavy` pairs (seed 9) |
| `data/valid.jsonl`, `data/test.jsonl` | the complex_v8 valid / test pairs (500 / 300), re-rendered (+ `*_meta.jsonl`) |
| `data_mix/` | what v9 trains on: the 7,251 pairs that use a window + 2,000 complex pairs without windows + 4,000 v7 pairs (13,251) |
| `data/split_stats.json` | counts and token lengths |
| `lora_config.yaml` | continues from v8 (`../complex_v8/adapters`), writes `complex_v9/adapters/` |
| `raw_pairs/`, `raw_extra/` | generated pairs + `verified.jsonl` (not tracked; rebuild below) |

All 15,500 generated pairs were executed in Spark again in the new style: 15,499 match (the one failure is the same
SQL error as in complex_v8). The extra pairs have non-empty results, no feature signature that complex_v8 holds out
for valid / test, and no SQL from an eval set, so `eval/complex_v8_test.jsonl` stays unseen and v8's scores on it stay
comparable.

## Rebuild

```bash
cd sql2pyspark-mlx/complex_v9
../.venv/bin/python ../complex_v8/generate.py --n 13000 --seed 8 --out raw_pairs --window-style mirror
../.venv/bin/python ../complex_v8/generate.py --n 2500 --seed 9 --out raw_extra --window-style mirror \
    --families window_heavy --prefix wx
../.venv/bin/python ../complex_v8/verify.py --src raw_pairs --workers 5 2>/dev/null    # ~26 min
../.venv/bin/python ../complex_v8/verify.py --src raw_extra --workers 5 2>/dev/null    # ~9 min
../.venv/bin/python build.py                                                           # -> data/ + data_mix/
```

## Train, export, evaluate

`zsh eval/start_training_v9.sh` (from the repo root) does all of it: stops Docker and Ollama models, trains, exports
`sql2pyspark:v9` (Q8_0) and `sql2pyspark:v9-q4` (Q4_K_M), and scores both on the 4 eval sets and the complex_v8 test
sample.

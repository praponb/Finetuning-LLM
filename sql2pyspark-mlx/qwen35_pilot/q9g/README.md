# q9g — one more full Qwen3.5-9B round, continued from q9df3 (2026-11-26 → 11-27) — q9gm-q4 promoted

The owner asked on 2026-11-26 for one more **full** training round on the Qwen3.5-9B model in use, with no new Kimi API
spend. The goal was to keep every promotion-rule number and raise **harness v2** (`eval/harness_suite_v2.jsonl`, 50
queries on nested types, maps, JSON, higher-order functions, semi / anti joins, set operations, interval frames, time
buckets, DECIMAL; test only). q9df3-q4 scored 6/50 there.

## Steps

1. **Step 0: exact starting point** ([`combine_adapters.py`](combine_adapters.py), [`step0.sh`](step0.sh),
   [`compare_fused.py`](compare_fused.py), [`step0.log`](step0.log)).
   - q9df3 = 0.5 q9d + 0.25 q9f + 0.25 q9f2.
   - The three rank-32 adapters have the same 124 modules and scale, so q9df3 is exactly one **rank-96 LoRA** on the
     4-bit base: `lora_a = concat(A_d, A_f, A_f2)` (last axis), `lora_b = concat(0.5·B_d, 0.25·B_f, 0.25·B_f2)` (first axis).
     This is `adapters_q9g_init/`.
   - Check A: fused base + init vs. `avg_weights.py` of the fused q9d / q9f / q9f2 models: max abs difference 1.95e-3,
     mean 3.4e-6 (bf16 rounding).
   - Check B: `sql2pyspark:q9g0-q4` scored hold-out 61, v4 21, fixed 147, the same as q9df3-q4.
2. **Free training pairs**, [`nested_v20/`](../../nested_v20/README.md).
   - 6,000 template pairs in 17 families aimed at the harness v2 failures. Each pair has its own small typed tables from
     the 30 training domains (never the held-out domains or the 13 suite tables).
   - 5,797 pairs passed the type-strict Spark check (column names + Spark types + rows + `ORDER BY` order), with 0
     mismatches.
3. **Training mix `data_q9g`**, [`polishmix/build_q9g.py`](../polishmix/build_q9g.py), [`build_q9g.log`](build_q9g.log).
   - Train: 12,011 lines. Valid: 837 (`data_q9e` valid + 100 nested_v20).

     | Part | Lines |
     | --- | --- |
     | nested_v20 (evened over the families) | 4,000 |
     | harness_v20 pilot (Kimi, 37 pairs × 3) | 111 |
     | `star_pool` (`SELECT *` + subquery; keeps the loop fix) | 400 |
     | replay from `data_q9e` | 7,500 |

   - Checks:
     - Eval SQL inside train: 0 of 2,195 eval SQL strings.
     - No new pair is near a harness query (token Jaccard < 0.5).
     - Valid / train overlap: 0.
     - Spark spot check: 200/200 nested, 200/200 replay.
4. **Training q9g**, [`../lora_config_q9g.yaml`](../lora_config_q9g.yaml), [`chain_q9g.sh`](chain_q9g.sh).
   - Settings: from `adapters_q9g_init`, 9,600 micro-batches (batch 1 × accumulation 4), lr 1e-5 (100-step warm-up)
     → cosine → 5e-7, `max_seq_length` 3,200.
   - Run: 2026-11-26 14:47 → 11-27 02:30 Bangkok time (11.7 h). 9 Metal restarts were auto-resumed. No divergence.
   - Val loss 0.012 → 0.004. Peak memory 16.5 GB (memory test: [`memtest.log`](memtest.log)).
5. **Candidates and quick gate** (all Q4_K_M; gate = 0 loops on the star probe, hold-out ≥ 61, v4 ≥ 20, fixed ≥ 145):
   - **q9g-q4**: the fused q9g model.
   - **q9gm-q4**: a 50/50 `avg_weights.py` merge of the fused q9g and fused q9df3 models.
   - **q9gw35-q4 / q9gw65-q4**: the same merge with q9g weight 0.35 / 0.65 ([`blends.sh`](blends.sh), owner
     2026-11-27). They got full evals only if they beat q9gm-q4; neither did.

| Model | Star probe (30) | Loops | Hold-out (62) | v4 (25) | Fixed (150) | Dev (100) | Harness v2 (50) | Gate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| v18a-q4 | 27 | 0 | 61 | 20 | 145 | 95 | 5 | pass |
| q9df3-q4 (before) | 28 | 0 | 61 | **21** | 147 | 95 | 6 | pass |
| q9g-q4 | 27 | 0 | 59 | 20 | 146 | 93 | 5 | **fail** |
| **q9gm-q4** | 28 | 0 | 61 | 20 | **149** | 95 | **8** | pass |
| q9gw35-q4 | 28 | 0 | 61 | 20 | 148 | **97** | 5 | pass |
| q9gw65-q4 | 28 | 0 | 61 | **21** | 148 | 93 | 7 | pass |

## Full evals (q9gm-q4)

| Eval set | v18a-q4 | q9df3-q4 (before) | **q9gm-q4** (`latest`) | Rule |
| --- | --- | --- | --- | --- |
| Hold-out (62) | 61 | 61 | **61** | ≥ 61 |
| v4 hold-out (25) | 20 | **21** | 20 | ≥ 20 |
| Fixed test (150) | 145 | 147 | **149** | ≥ 145 |
| complex_v8 test (100) | 88 | 93 | **98** | ≥ 88 |
| Complex schema-shift (150 of 300) | 126 | 131 | **135** | ≥ 126 |
| Schema-shift (200) | 186 | 186 | **187** | no clear drop |
| targeted_v11 / v12 held-out (300 / 250) | 281 / 232 | 295 / 247 | **297** / **249** | no clear drop |
| targeted_v13 / v14 held-out (150 / 200) | 148 / 195 | **150** / **200** | **150** / **200** | no clear drop |
| `IN` / `NOT IN` (50) | 50 | 50 | 50 | |
| Failures, all sets / with a name slip | 179 / 66 | 130 / **32** | **115** / 34 | slips ≤ 53 |
| Harness v1 (21), type-strict | 5 | 5 | **6** | |
| Harness v2 (50), type-strict | 5 | 6 | **8** | |
| Real queries (4) / real-style pairs (74) | 0 / 0 | 0 / 0 | 0 / 0 | |

Harness v2 in detail:

| Model | All (50) | Part 1 (21) | Part 2 (29) | Code runs | Syntax errors | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| v18a-q4 | 5 | 5 | 0 | 7 | 7 | Q04b, Q04c, Q06b, Q09a, Q10a |
| q9df3-q4 | 6 | 5 | 1 | 10 | 19 | Q04b, Q04c, Q06b, Q09b, Q11a, Q18a |
| q9g-q4 | 5 | 5 | 0 | 7 | 17 | Q04c, Q06b, Q09b, Q10a, Q10b |
| **q9gm-q4** | **8** | 6 | **2** | 9 | 15 | Q04b, Q04c, Q06b, Q09a, Q09b, Q10b, Q13b, Q18a |
| q9gw35-q4 | 5 | 5 | 0 | 7 | 16 | Q04b, Q04c, Q06b, Q09a, Q09b |
| q9gw65-q4 | 7 | **7** | 0 | 7 | 16 | Q04b, Q04c, Q06b, Q09a, Q09b, Q10a, Q10b |

q9gm-q4 passes the promotion rule against v18a-q4. Against q9df3-q4 it is better or equal on every set except v4 (20 vs.
21; the owner accepted this on 2026-11-27) and name slips (34 vs. 32, still under 53).

Harness v2 is still mostly unsolved: Part 2 is 2/29. Most answers still fail before the results can be compared, for
example:
- `GroupedData.pivot()` called with the wrong arguments
- `'Column' object is not callable`
- unresolved column names
- broken indentation in long answers

The template pairs were short single-statement forms, which helps only a little on the long multi-CTE suite queries.

## Promotion (2026-11-27)

The owner pre-approved promotion for this round only.

- **12:02 Bangkok time:** `ollama cp sql2pyspark:q9gm-q4 sql2pyspark:latest` (`20b780359182`).
- **Root GGUF:** `sql2pyspark-q4.gguf` = q9gm-q4. SHA-256 `c2cf4ec7…`, the same as `variants/gguf/q9gm-q4.gguf`.
  `sql2pyspark.gguf` stays v18a Q8_0.
- **Before promotion:** the patched `app.py` copy passed on port 8512 ([`../variants/apptest/`](../variants/apptest/)):
  AppTest 7/7 (including 4 `SELECT *` + `[NOT] IN` / `[NOT] EXISTS`) and the long-input / cut-off test. Health was ok.
- **After promotion:** AppTest on the live `app.py` with model `sql2pyspark` (Kimi off): 7/7, 2–5 s each. Long-input
  test passed. Health 200. An anonymous request to the site gets a 302 to Cloudflare Access.
- **Rollback:**
  ```bash
  ollama cp sql2pyspark:q9df3-q4 sql2pyspark:latest
  cp sql2pyspark-mlx/qwen35_pilot/variants/gguf/q9df3-q4.gguf sql2pyspark-q4.gguf
  ```
  The q9df3 GGUF has SHA-256 `f952a900…`.

Kept:
- `adapters_q9g_init/` and `adapters_q9g/`. Their safetensors are git-ignored, ~520 MB each.
- The fused models `variants/work/hf_q9g`, `hf_q9g0`, `hf_q9df3`, `hf_q9d`, `hf_q9f` and `hf_q9f2`.
- The GGUFs and Ollama tags `q9g0-q4`, `q9g-q4`, `q9gm-q4`, `q9gw35-q4` and `q9gw65-q4`.

## Files

| File | What |
| --- | --- |
| `combine_adapters.py`, `step0.sh`, `compare_fused.py`, `step0.log` | Step 0 (exact rank-96 q9df3 adapter, checks A + B) |
| `memtest.yaml`, `memtest.log` | 50-step memory test |
| `build_q9g.log` | Data mix build log (`polishmix/build_q9g.py`) |
| `chain_q9g.sh`, `chain_q9g.log` | Train → export → gate → full evals → name slips (log cleaned of progress bars / Spark JSON lines) |
| `blends.sh`, `blends.log` | Extra blends at q9g weight 0.35 / 0.65 |
| `../variants/gate_q9g.py` | Quick-gate table with harness v2 |
| `../lora_config_q9g.yaml` | Training config |
| `../data_q9g/` | Training mix (train / valid / stats.json) |

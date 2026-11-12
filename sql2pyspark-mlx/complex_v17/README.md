# complex_v17 — v15's full mix + 4,000 complex pairs (v17, not promoted)

Training data and config for **v17**, the follow-up to [v16](../complex_v16/README.md) and the first training run on
the Mac mini M6 (2026-11-08).

## Why

v16 continued v15 on 10,000 complex pairs on the synthetic catalog (48% of its mix). It won the complex schema-shift set
(128/150 vs. v15-q4 110) but lost on the earlier forms (hold-out 58, complex test 84, v11 / v12 held-out down) because
the replay share fell. v17 keeps **v15's whole replay** and adds a **smaller complex share**.

## Data ([`build.py`](build.py), seed 17)

| Part | Pairs |
| --- | --- |
| `schema_v15/data_mix/train.jsonl` (v15's exact mix) | 25,008 |
| sampled from `complex_v16/data/train.jsonl` (the complex schema-shift set is already excluded) | 4,000 |
| **train** (`data_mix/train.jsonl`, shuffled) | **29,008** (complex share 13.8%, mean 1,980 characters) |
| valid (v16's, unchanged) | 337 (0 overlap with train) |

`../.venv/bin/python build.py` rebuilds `data_mix/` and `data_mix/stats.json`.

## Training ([`lora_config.yaml`](lora_config.yaml), [`train_v17.log`](train_v17.log))

v15's config unchanged: resumes `schema_v15/adapters`, 4,800 micro-batches (batch 1 × 4 accumulation), cosine
`[5e-5, 1150, 5e-6]`, last 8 of 28 blocks, seed 17. Run by [`eval/start_training_v17.sh`](../../eval/start_training_v17.sh) (train →
Q8_0 + Q4_K_M export → all eval suites → name slips), log `eval/start_training_v17.log`.

On the Mac mini M6 (32 GB): training 2026-11-08 16:03 → 19:05 (**~3 h**; the M2 estimate was ~10.3 h), 137–156
tokens/s, peak 10.1 GB, no restarts; validation loss 0.009 → 0.013 (peak learning rate) → 0.008. The whole chain
finished at 22:30. Tags `sql2pyspark:v17`, `sql2pyspark:v17-q4`.

## Results (q4)

| Eval set | **v15-q4** (`latest`) | v16-q4 | **v17-q4** |
| --- | --- | --- | --- |
| Hold-out (62) | 60 | 58 | **61** |
| v4 hold-out (25) | 20 | 20 | **21** |
| Fixed test (150; classic `subquery` 17/17 for all three) | 145 | **146** | 142 |
| complex_v8 test (100 of 300) | **89** | 84 | 85 |
| Complex schema-shift (150 of 300) | 110 | **128** | 116 |
| Schema-shift (200) | **189** | 185 | **189** |
| Held-out `IN` / `NOT IN` (50) | 50 | 50 | 50 |
| Held-out targeted_v11 / v12 / v13 / v14 | 278 / 232 / 150 / 193 | 261 / 216 / 149 / 190 | **283** / **233** / **150** / **197** |
| Failures in total / with an unresolved name | 121 / 62 | 150 / 83 | **110** / **58** |

**Verdict: not promoted.** v17 has the best hold-out and the fewest failures so far, but the promotion rule (hold-out
≥ 60, classic `subquery` 17/17, no loss on v4 / fixed / complex) fails on the fixed test (−3) and the complex test (−4),
and complex-shift +6 is not clearly above v15. `sql2pyspark:latest` stays **v15-q4**. Note: the export overwrote the
root `sql2pyspark.gguf` / `sql2pyspark-q4.gguf`, which now hold v17 / v17-q4 (restore v15 from the Ollama blobs before a
server deploy with `deploy/update_model.sh`).

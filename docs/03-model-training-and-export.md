# 03 - Model Training & Export Pipeline

This document covers fine-tuning, weight fusion, dequantization, GGUF conversion and Ollama packaging for the SQL → PySpark model.

---

## 🧠 Base Model Selection

The project has used two base models in the `Qwen2.5-Coder` family:

| Version | Base Model | Layers | Parameters | Context / Seq Len | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **v1–v5** | `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit` | 36 transformer blocks | 3.09 B | 3,200 tokens (v4/v5) | Fast training (~5.5 h) and fast serving (~3-4 s/query). Solves 95% of hold-out queries. |
| **v6–v7** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | Higher capacity for long, multi-CTE pipelines and complex nested expressions (v7: 61/62 holdout, 137/150 fixed test). |
| **v8** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | Fine-tuned on `complex_v8` (10,000 new complex pairs + 7,000 v7 mix, 20 new tables in 6 domains). |
| **v9** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | Fine-tuned on `complex_v9` (mirror window style, 13,251 mix) against v8's many-window failures; exported as Q8_0 (`v9`) and Q4_K_M (`v9-q4` = `latest` since 2026-10-01). |
| **v10** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v9 continued on `classic_v10` (classic pairs in mirror window style + bare `IN` / `NOT IN`, 12,463 mix); exported as Q8_0 (`v10`) and Q4_K_M (`v10-q4`, candidate, not promoted). |
| **v11** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v10 continued on `targeted_v11` (correlated subqueries with outer conditions in the join, set operations, `LATERAL VIEW` filters; 15,000 mix); Q8_0 (`v11`) and Q4_K_M (`v11-q4`, `latest` on the Mac 2026-10-02 → 10-03). |
| **v12** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v11 continued on `targeted_v12` (`OR` / mixed `AND`-`OR` conditions + replay; 12,976 mix); Q8_0 (`v12`) and Q4_K_M (`v12-q4` = `latest` on the Mac since 2026-10-03). |
| **v13** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v12 continued on `targeted_v13` (uncorrelated scalar subqueries + replay; 11,500 mix); Q8_0 (`v13`) and Q4_K_M (`v13-q4`); a regression, not used. |
| **v14** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v12 continued on `targeted_v14` (correlated + uncorrelated scalar subqueries side by side + replay; 25,008 mix); Q8_0 (`v14`), Q4_K_M (`v14-q4`) and the iter-2,400 checkpoint (`v14-c2400-q4`); not promoted. |
| **v15** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v14 continued on `schema_v15` (the existing forms on 221 synthetic tables with unseen names + replay; 25,008 mix); Q8_0 (`v15`) and Q4_K_M (`v15-q4` = `latest` on the Mac since 2026-10-09). |
| **v16** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | v15 continued on `complex_v16` (10,000 complex pairs on the synthetic tables + replay; 21,008 mix); Q8_0 (`v16`) and Q4_K_M (`v16-q4`); not promoted. |

### Why scale from 3B to 7B in v6?

Across v1 through v5, 3B models achieved 95% accuracy on classic queries and 56% on v4 features, but scored 0/4 on real-world queries due to repetition loops (`real001` repeating window definitions until token exhaustion) and syntax slips under heavy nesting. While Kimi K3 repair fixes these in post-processing, a larger 7B base model provides the intrinsic representation capacity needed to generate dense, multi-stage pipelines end-to-end.

---

## ⚙️ LoRA Fine-Tuning Setup (`lora_config.yaml`)

LoRA (Low-Rank Adaptation) freezes the pretrained weights and trains small low-rank matrices added to the attention and MLP projections of the adapted layers.

Training is configured in [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) and run with `make train`, which calls [train_resumable.py](../sql2pyspark-mlx/train_resumable.py) (see [Crash-proof training](#crash-proof-training-train_resumablepy)).

### Current v6 Configuration (7B Base Model)

```yaml
# LoRA fine-tune of Qwen2.5-Coder-7B-Instruct (4-bit MLX) on the SQL -> PySpark dataset.
# v6: 7B model, fresh adapters (peak 1e-4) on the v5 replay-heavy mix.
model: mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
train: true
fine_tune_type: lora
data: ./data                 # train.jsonl / valid.jsonl / test.jsonl
adapter_path: ./adapters
seed: 0

# 1,200 optimizer updates of 4 samples, done as 4,800 micro-batches of 1 with gradient accumulation
iters: 4800
batch_size: 1
grad_accumulation_steps: 4
max_seq_length: 3200          # longest training sample: 3,119 tokens
mask_prompt: true            # loss only on the assistant (PySpark) tokens
grad_checkpoint: true        # essential for 7B memory constraints

num_layers: 8                # adapt the last 8 of 28 transformer blocks (3B used 8 of 36)
lora_parameters:
  rank: 16
  dropout: 0.05
  scale: 20.0

optimizer: adamw
learning_rate: 1.0e-4          # fresh adapters schedule
lr_schedule:
  name: cosine_decay
  warmup: 100
  warmup_init: 1.0e-6
  arguments: [1.0e-4, 1100, 1.0e-5]   # [init, decay_steps, end] (optimizer updates)

steps_per_report: 50
steps_per_eval: 1200          # 4 evaluation passes across 4,800 iterations
val_batches: 100             # 100 held-out samples per eval (batch 1)
save_every: 200               # checkpoints saved every 200 iterations
test: false
test_batches: 200
```

### Hyperparameter Evolution Across Versions

1. **v2 / v3 (3B)**: `iters: 1200`, `batch_size: 4`, no accumulation, `max_seq_length: 1024`, no gradient checkpointing, peak ~9.7 GB, ~1 h 50 min.
2. **v4 (3B)**: `iters: 2400`, `batch_size: 2`, `grad_accumulation_steps: 2`, `max_seq_length: 3200`, gradient checkpointing enabled, peak 9.74 GB, ~5 h 16 min.
3. **v5 (3B)**: continued from v4 (`resume_adapter_file: ./runs/v4/adapters/adapters.safetensors`), half learning rate (5.0e-5), peak 10.31 GB, ~5 h 39 min.
4. **v6 (7B)**: `iters: 4800`, `batch_size: 1`, `grad_accumulation_steps: 4`, 8 of 28 layers adapted, gradient checkpointing. Dry run on the 64 longest samples showed `batch_size: 2` caused Metal OOM; `batch_size: 1` with accumulation 4 peaked at **9.0 GB** in the dry run and **10.1 GB** in the full run, within the ~12.7 GB Metal working set of a 16 GB Mac, at ~37–50 trained tokens/s (**8 h 19 min** in total, no crash).
5. **v7 (7B, continued from v6)**: `resume_adapter_file: ./runs/v6/adapters/adapters.safetensors`, `iters: 2400` (600 updates of 4 samples), `batch_size: 1`, `grad_accumulation_steps: 4`, peak LR 5.0e-5 (half of v6), warmup 50, cosine decay to 5.0e-6 over 550 updates. 4 h 58 min on a base M2, peak 9.9 GB, validation loss 0.011 → 0.021 (step 800) → 0.010 → **0.009**.
6. **v8 (7B, continued from v7, `complex_v8/lora_config.yaml`)**: `data: ./complex_v8/data_mix` (10,000 new complex pairs + 7,000 v7 pairs), `resume_adapter_file: ./adapters/adapters.safetensors` (v7), `adapter_path: ./complex_v8/adapters`, `iters: 3200` (800 updates of 4), same batch / accumulation / LR as v7, cosine to 5.0e-6 over 750 updates. Samples average ~881 tokens (v7: 486), so each step is slower: **11 h 42 min**, peak 10.0 GB, validation loss 0.135 → 0.021 (step 800) → 0.016 → 0.012 → **0.011**.
7. **v9 (7B, continued from v8, `complex_v9/lora_config.yaml`)**: `data: ./complex_v9/data_mix` (13,251 pairs in mirror window style), `resume_adapter_file: ./complex_v8/adapters/adapters.safetensors` (v8), `adapter_path: ./complex_v9/adapters`, `iters: 2400` (600 updates of 4), `batch_size: 1`, `grad_accumulation_steps: 4`, cosine to 5.0e-6 over 550 updates. Final validation loss **0.008**, training loss **0.003**, peak memory 10.0 GB.
8. **v10 (7B, continued from v9, `classic_v10/lora_config.yaml`)**: `data: ./classic_v10/data_mix` (12,463 pairs), `resume_adapter_file: ./complex_v9/adapters/adapters.safetensors` (v9), `adapter_path: ./classic_v10/adapters`, `iters: 2400`, seed 10, otherwise as v9. 6 h 24 min, peak 9.9 GB, validation loss 0.013 → 0.012 → 0.006 → **0.007**.
9. **v11 (7B, continued from v10, `targeted_v11/lora_config.yaml`)**: `data: ./targeted_v11/data_mix` (15,000 pairs: 10,000 targeted subquery/setop/lateral pairs + 5,000 sampled from classic_v10), `resume_adapter_file: ./classic_v10/adapters/adapters.safetensors` (v10), `adapter_path: ./targeted_v11/adapters`, `iters: 4000` (1,000 optimizer updates of 4 samples), `batch_size: 1`, `grad_accumulation_steps: 4`, peak LR 5.0e-5, warmup 50, cosine decay to 5.0e-6 over 950 updates, seed 11. Mean sample length ~1,430 chars. 6 h 04 min (2026-10-02 05:55 → 11:59), peak 9.7 GB, validation loss 0.072 → 0.009 → 0.008 → 0.007 → **0.007**.

### Key Architectural Choices

- **Batch 1 × 4 accumulation + gradient checkpointing (7B since v6)**: on a 16 GB Mac, batch 2 runs out of Metal memory. Batch 1 with gradient accumulation 4 keeps peak memory at 9.0–10.1 GB.
- **Masked prompt (`mask_prompt: true`)**: the system message and SQL prompt are excluded from the loss, so updates focus on generating the PySpark answer.
- **Cosine decay with warmup**: v6 used 100 warmup updates to $10^{-4}$ decaying to $10^{-5}$ over 1,100 updates; the continuations since v7 use 50 warmup updates to $5 \times 10^{-5}$ decaying to $5 \times 10^{-6}$ (v7/v9/v10: over 550 updates, v8: 750, v11: 950).

### Crash-proof training (`train_resumable.py`)

The first v4 run crashed after ~250 iterations with `[METAL] Command buffer execution failed: Impacting Interactivity`: macOS aborts a GPU command buffer that runs too long, and mlx_lm cannot recover. `train_resumable.py`:

- runs `mlx_lm.lora` with smaller Metal command buffers (`MLX_MAX_OPS_PER_BUFFER=2`, `MLX_MAX_MB_PER_BUFFER=40`; ~9% slower), which avoided the error for the whole second run;
- if the run still dies, restarts from the newest checkpoint (saved every 100 iterations) for the remaining iterations, continuing the cosine learning-rate schedule from where it stopped (AdamW moments restart), up to 6 times.

---

## 📊 Training Dynamics (`train.log`)

### v11 (7B, continued from v10, targeted_v11 data mix)

Run with `train_resumable.py --config targeted_v11/lora_config.yaml` from `eval/start_training_v11.sh` (log `sql2pyspark-mlx/targeted_v11/train_v11.log`). 2026-10-02 05:55 → 11:59 (6 h 04 min) without a crash; adapters in `sql2pyspark-mlx/targeted_v11/adapters/`. The short new pairs make each step faster than v10's (0.18–0.25 it/s).

```text
v11 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.072       —             —                —               —                 —
50       —           0.086         1.178e-5         0.179           10,700            8.56 GB
1000     0.009       0.013         4.535e-5         0.199           229,945           9.61 GB
2000     0.008       0.008         2.951e-5         0.180           460,798           9.61 GB
3000     0.007       0.006         1.237e-5         0.237           693,837           9.72 GB
4000     0.007       0.004         5.000e-6         0.205           914,586           9.72 GB
```

The high starting validation loss (0.072, v10 started its run at 0.013) shows the targeted forms were new to v10.

### v16 (7B, continued from v15, complex_v16 data mix) — not promoted

Run with `train_resumable.py --config complex_v16/lora_config.yaml` from `eval/start_training_v16.sh` (log `sql2pyspark-mlx/complex_v16/train_v16.log`): 21,008 pairs, resumes `schema_v15/adapters`, 3,600 micro-batches (cosine to 5e-6 over 850 updates), `val_batches: 120`, seed 16. 2026-10-08 06:55 → 18:49 (11 h 54 min; the complex pairs are long, mean 1,283 tokens), no sleep under `caffeinate`.

```text
v16 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.008       —             —                —               —                 —
900      0.012       0.015         4.556e-5         0.076           398,340           10.00 GB
1800     0.007       0.009         2.974e-5         0.062           812,179           10.00 GB
2700     0.006       0.007         1.247e-5         0.105           1,229,346         10.00 GB
3600     0.004       0.008         5.001e-6         0.103           1,633,719         10.04 GB
```

The lowest final validation loss so far, but the validation set is mostly the new complex pairs: the gain shows on the complex schema-shift set (128/150 vs. v15-q4 110) while the hold-out (58), the complex test (84) and the v11 / v12 held-out pairs fell. See [Evaluation](04-evaluation-and-benchmarking.md#-v16-results-2026-10-09--not-promoted).

### v15 (7B, continued from v14, schema_v15 data mix) — `latest` on the Mac

Run with `train_resumable.py --config schema_v15/lora_config.yaml` from `eval/start_training_v15.sh` (log `sql2pyspark-mlx/schema_v15/train_v15.log`): 25,008 pairs, resumes `targeted_v14/adapters`, 4,800 micro-batches (cosine to 5e-6 over 1,150 updates), `val_batches: 150`, seed 15. 2026-10-07 13:18 → 21:27 (8 h 09 min), the Mac kept awake by `caffeinate -s -i -w <chain pid>` inside the script.

```text
v15 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.011       —             —                —               —                 —
800      0.019       0.018         4.819e-5         0.197           204,561           9.68 GB
1600     0.009       0.010         4.058e-5         0.238           389,732           9.68 GB
2400     0.008       0.010         2.916e-5         0.187           581,906           9.74 GB
3200     0.007       0.009         1.726e-5         0.273           776,863           9.78 GB
4000     0.007       0.005         8.340e-6         0.199           964,281           9.81 GB
4800     0.007       0.004         5.000e-6         0.207           1,167,603         10.04 GB
```

The low starting loss (0.011) shows v14 already handled these forms on new names in-distribution; the bump at iter 800 is the peak learning rate. The gains show where validation cannot: unseen hold-out schemas (60/62) and the schema-shift domains (189/200).

### v14 (7B, continued from v12, targeted_v14 data mix) — not promoted

Run with `train_resumable.py --config targeted_v14/lora_config.yaml` from `eval/start_training_v14.sh` (log `sql2pyspark-mlx/targeted_v14/train_v14.log`): 25,008 pairs, resumes `targeted_v12/adapters` (not v13's), 4,800 micro-batches (1,200 updates, cosine to 5e-6 over 1,150), `val_batches: 150`, seed 14. 2026-10-06 14:21 → 10-07 00:47; about 2.5 h of that the Mac was asleep (training only advanced in short DarkWakes until it was woken), so the chain now runs under `caffeinate -s -i -w <pid>`.

```text
v14 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.055       —             —                —               —                 —
800      0.015       0.016         4.819e-5         0.209           200,408           9.81 GB
1600     0.010       0.012         4.058e-5         0.222           403,573           9.81 GB
2400     0.008       0.006         2.916e-5         0.222           601,185           9.81 GB
3200     0.006       0.006         1.726e-5         0.165           786,162           9.93 GB
4000     0.006       0.007         8.340e-6         0.107           1,000,507         9.93 GB
4800     0.006       0.005         5.000e-6         0.192           1,204,576         10.00 GB
```

The starting loss (0.055) shows the correlated forms were new to v12. This time the validation set held the classic scalar-subquery pairs, and the classic `subquery` family did reach 17/17; the regression v14 does have (names copied from training tables into unseen schemas) is not something an in-distribution validation set can show. The iter-2,400 checkpoint, exported by `eval/eval_v14_ckpt.sh 2400` as `sql2pyspark:v14-c2400-q4`, has the same slips.

### v13 (7B, continued from v12, targeted_v13 data mix) — not used

Run with `train_resumable.py --config targeted_v13/lora_config.yaml` from `eval/start_training_v13.sh` (log `sql2pyspark-mlx/targeted_v13/train_v13.log`): 11,500 pairs, resumes `targeted_v12/adapters`, 2,400 micro-batches, seed 13. 2026-10-03 06:29 → 11:38 (5 h 09 min), peak 10.0 GB.

```text
v13 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.010       —             —                —               —                 —
800      0.010       0.012         4.243e-5         0.129           282,000           9.76 GB
1600     0.008       0.012         1.839e-5         0.165           553,556           10.00 GB
2400     0.007       0.006         5.001e-6         0.145           801,226           10.00 GB
```

The validation loss looked like v12's, yet the model regressed on the hold-out (52/62) and the classic subquery family: the validation set (75 new + 300 v12 pairs) had none of the correlated scalar subqueries v13 broke, so a low validation loss did not catch it.

### v12 (7B, continued from v11, targeted_v12 data mix)

Run with `train_resumable.py --config targeted_v12/lora_config.yaml` from `eval/start_training_v12.sh` (log `sql2pyspark-mlx/targeted_v12/train_v12.log`): `data: ./targeted_v12/data_mix` (12,976: 4,700 new pairs with `OR` + 976 classic `OR` pairs + 2,800 classic + 2,500 complex_v9 + 2,000 targeted_v11), `resume_adapter_file: ./targeted_v11/adapters/adapters.safetensors`, 2,400 micro-batches (600 updates, cosine to 5e-6 over 550), seed 12. 2026-10-02 18:55 → 23:47 (4 h 52 min) without a crash; adapters in `sql2pyspark-mlx/targeted_v12/adapters/`.

```text
v12 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.012       —             —                —               —                 —
50       —           0.019         1.178e-5         0.163           14,546            9.31 GB
800      0.014       0.017         4.243e-5         0.175           226,888           9.87 GB
1600     0.008       0.010         1.839e-5         0.176           456,991           9.91 GB
2400     0.007       0.007         5.001e-6         0.172           664,033           9.91 GB
```

### v9 (7B, continued from v8, complex_v9 data mix)

Run with `train_resumable.py --config complex_v9/lora_config.yaml` from `eval/start_training_v9.sh` (log `sql2pyspark-mlx/complex_v9/train_v9.log`). 2026-09-30 19:05 → 2026-10-01 05:23 (10 h 18 min) without a crash; adapters in `sql2pyspark-mlx/complex_v9/adapters/`, so `./adapters` and `complex_v8/adapters` are untouched.

```text
v9 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.034       —             —                —               —                 —
50       —           0.019         1.178e-5         0.083           27,091            9.19 GB
800      0.018       0.017         4.243e-5         0.066           493,491           10.00 GB
1600     0.007       0.008         1.839e-5         0.072           980,699           10.00 GB
2400     0.008       0.003         5.001e-6         0.073           1,465,079         10.00 GB
```

Final training loss reached **0.003**, with validation loss at **0.008**. The model trained on 1,465,079 tokens over 2,400 iterations, with adapters safely saved in `complex_v9/adapters/adapters.safetensors`.

### v10 (7B, continued from v9, classic_v10 data mix)

Run with `train_resumable.py --config classic_v10/lora_config.yaml` from `eval/start_training_v10.sh` (log `sql2pyspark-mlx/classic_v10/train_v10.log`). 2026-10-01 12:18 → 18:42 (6 h 24 min) without a crash; adapters in `sql2pyspark-mlx/classic_v10/adapters/`. Samples are shorter than v9's (mean ~793 tokens), so the run took 4 h less for the same 2,400 steps.

```text
v10 iter Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.013       —             —                —               —                 —
50       —           0.027         1.178e-5         0.095           24,889            9.31 GB
800      0.012       0.009         4.243e-5         0.108           354,198           9.87 GB
1600     0.006       0.005         1.839e-5         0.104           679,875           9.93 GB
2400     0.007       0.006         5.001e-6         0.105           1,004,359         9.93 GB
```

The low starting validation loss (0.013) shows v9 already knew most of the data; the loss that dropped is the mirrored classic window code and the new `IN` / `NOT IN` form.

### v8 (7B, continued from v7, complex_v8 data mix)

Run with `train_resumable.py --config complex_v8/lora_config.yaml` from `eval/start_training_v8.sh` (log `sql2pyspark-mlx/complex_v8/train_v8.log`; v7 backed up to `runs/v7/` first). 2026-09-29 15:05 → 02:47 (11 h 42 min) without a crash; adapters in `sql2pyspark-mlx/complex_v8/adapters/`, so `./adapters` (v7) is untouched.

```text
v8 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.135       —             —                —               —                 —
50       —           0.070         1.178e-5         0.097           20,775            9.33 GB
800      0.021       0.017         4.581e-5         0.078           366,107           10.04 GB
1600     0.016       0.018         3.004e-5         0.074           752,257           10.04 GB
2400     0.012       0.014         1.259e-5         0.082           1,131,486         10.04 GB
3200     0.011       0.010         5.001e-6         0.079           1,507,570         10.04 GB
```

The starting validation loss of 0.135 (v7 on its own data: 0.011) shows how new the complex_v8 queries were to v7. Each validation pass over 100 of these long samples took 8–14 minutes. Export (`mlx_lm.fuse --adapter-path complex_v8/adapters`, Q8_0 GGUF, `ollama create sql2pyspark:v8`) ran from `eval/post_training_v8.sh`; `sql2pyspark:v8` = `aa01d9d717f7`. Evaluation results: [v8 results](04-evaluation-and-benchmarking.md#-v8-results-2026-09-30).

### v7 (7B, continued from v6)

`sql2pyspark-mlx/train.log` is now the **v7** run (the v6 log is in `sql2pyspark-mlx/runs/v6/train.log`): `resume_adapter_file: ./runs/v6/adapters/adapters.safetensors`, peak learning rate 5e-5 (50 warmup updates, cosine to 5e-6), 600 updates of 4 samples on the v7 dataset as 2,400 micro-batches of 1. 18:13 → 23:11 (4 h 58 min) without a crash; peak memory 9.93 GB.

```text
v7 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.011       —             —                —               —                 —
50       —           0.010         1.178e-5         0.165           13,584            7.51 GB
800      0.021       0.015         4.243e-5         0.118           203,460           8.59 GB
1600     0.010       0.011         1.839e-5         0.118           404,257           9.93 GB
2400     0.009       0.008         5.001e-6         0.126           597,072           9.93 GB
```

The validation set is v7's (it contains the new patterns), so its values are not comparable with v6's table. Validation loss rose while the learning rate was near its peak (continued training restarts the optimizer state) and fell below the starting value as the rate decayed. Evaluation results: [v7 results](04-evaluation-and-benchmarking.md#-v7-results-2026-09-29).

### v6 (7B, fresh adapters)

The **v6** run (log in `sql2pyspark-mlx/runs/v6/train.log`; the v5 log is in `sql2pyspark-mlx/runs/v5/train.log`): `Qwen2.5-Coder-7B-Instruct-4bit`, fresh adapters, peak learning rate 1e-4 (100 warmup updates, cosine to 1e-5), 1,200 updates of 4 samples on the v5 dataset as 4,800 micro-batches of 1. 07:09 → 15:28 (8 h 19 min) without a crash, including five 4.5–7 min validation passes over 100 samples; peak memory 10.11 GB.

```text
v6 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.289       —             —                —               —                 —
50       —           0.307         1.189e-5         0.174           10,755            7.67 GB
100      —           0.077         2.476e-5         0.139           23,507            8.43 GB
1200     0.020       0.029         9.299e-5         0.173           301,354           9.23 GB
2400     0.013       0.014         6.166e-5         0.198           606,530           9.40 GB
3600     0.011       0.007         2.573e-5         0.157           923,387           10.11 GB
4800     0.010       0.008         1.000e-5         0.139           1,236,571         10.11 GB
```

Iterations are micro-batches of 1 sample, so the it/s column is not comparable with the 3B runs (2 samples per micro-batch). On the same validation set as v5, v6 ends at 0.010 vs. v5's 0.012, although v5 had v4's training before its own. Evaluation results: [v6 results](04-evaluation-and-benchmarking.md#-v6-results-2026-09-28).

### v5 (continued from v4)

The **v5** run (log in `sql2pyspark-mlx/runs/v5/train.log`): `resume_adapter_file: ./runs/v4/adapters/adapters.safetensors`, peak learning rate 5e-5 (50 warmup updates, cosine to 5e-6), 1,200 updates of 4 samples on the v5 mix (55% classic, 25% short v4 features, 20% pipelines). 17:17 → 22:56 (5 h 39 min) without a crash; peak memory 10.31 GB.

```text
v5 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.030       —             —                —               —                 —
50       —           0.017         2.452e-5         0.138           23,532            8.69 GB
600      0.020       0.018         4.503e-5         0.116           304,350           10.31 GB
1200     0.017       0.014         2.916e-5         0.120           607,853           10.31 GB
1800     0.014       0.012         1.223e-5         0.105           913,624           10.31 GB
2400     0.012       0.008         5.000e-6         0.139           1,211,697         10.31 GB
```

The v4 run below trained from scratch; its log is in `sql2pyspark-mlx/runs/v4/train.log`.

### v4 (from scratch)

The **v4** run (restarted with `train_resumable.py` at 09:47 after the Metal crash; finished at 15:04 without another crash). Iterations are micro-batches of 2 samples; the v2 and v3 logs are kept in `sql2pyspark-mlx/runs/v2/` and `runs/v3/`. Measured on a base Apple M2 (16 GB):

```text
v4 iter  Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
───────────────────────────────────────────────────────────────────────────────────────────────
1        0.389       —             —                —               —                 —
50       —           0.192         2.476e-5         0.104           19,303            7.13 GB
100      —           0.070         4.951e-5         0.086           41,904            7.67 GB
600      0.050       0.029         9.299e-5         0.119           243,556           8.77 GB
1200     0.025       0.020         6.166e-5         0.137           498,531           9.29 GB
1800     0.024       0.011         2.573e-5         0.180           756,069           9.74 GB
2400     0.018       0.012         1.000e-5         0.179           1,003,159         9.74 GB
```

For comparison, v3 went from val loss 0.547 to 0.013 / 0.008 / 0.005 / **0.005** at steps 300 / 600 / 900 / 1200 (batch 4, peak 9.70 GB), and v2 to **0.004**. Each version has its own validation set (the split is recomputed per dataset), and v4's contains long pipelines and nested data, so the higher v4 loss is not a regression.

### Observations

- **Convergence**: training loss fell below 0.07 within 100 micro-batches and to ~0.012 at the end; validation loss flattened around 0.024 between iterations 1,200 and 1,800 and ended at 0.018.
- **Memory**: peak 9.74 GB with gradient checkpointing. Running Ollama inference or Docker Desktop at the same time is not possible on a 16 GB Mac.
- **Duration**: 5 h 16 min for 2,400 micro-batches (≈ 4,800 samples, ~1.0 M trained tokens) including five ~3–5 min validation passes; ~2.5× the v3 run because the average v4 sample is 1.8× longer and checkpointing costs ~40%.

Checkpoints are saved every 100 iterations in `sql2pyspark-mlx/adapters/` (`0000100_adapters.safetensors` … `0002400_adapters.safetensors`; the last equals `adapters.safetensors`).

---

## 🔄 Adapter Fusion & Dequantization (`make fuse`)

Two fused copies are produced:

### Step 1: 4-bit Native MLX Model

```bash
mlx_lm.fuse \
  --model mlx-community/Qwen2.5-Coder-3B-Instruct-4bit \
  --adapter-path adapters \
  --save-path sql2pyspark-model
```

Saved in `sql2pyspark-mlx/sql2pyspark-model/` (1.74 GB `model.safetensors`), usable directly with `mlx_lm.generate`.

### Step 2: Dequantized BF16 Copy for llama.cpp

`llama.cpp` quantizes from 16-bit weights, so the 4-bit base and the adapters are fused and dequantized:

```bash
mlx_lm.fuse \
  --model mlx-community/Qwen2.5-Coder-3B-Instruct-4bit \
  --adapter-path adapters \
  --save-path ../sql2pyspark-hf \
  --dequantize
```

Saved in `sql2pyspark-hf/` (BF16): ~15.2 GB for the 7B (v6: `model-00001-of-00003` … `model-00003-of-00003.safetensors`, 5.34 + 5.26 + 4.62 GB), ~6.2 GB for the 3B (two shards, 5.36 GB + 816 MB), plus `tokenizer.json`, `config.json`, `chat_template.jinja` and `model.safetensors.index.json`. `mlx_lm.fuse` doesn't delete shards of an earlier export with a different shard count; only the files named in the index are read.

Note that these weights started as 4-bit, so the BF16 copy carries the 4-bit rounding; dequantizing doesn't restore full precision.

---

## 📦 GGUF Conversion (`make gguf`)

The BF16 copy is converted to **Q8_0 (8-bit)** with `convert_hf_to_gguf.py` from `llama.cpp`:

```bash
python llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf \
  --outtype q8_0 \
  --outfile sql2pyspark.gguf
```

It takes about 80 seconds.

### Quantization Formats: Q8_0 & Q4_K_M

- **Q8_0 (8-bit)**: `sql2pyspark.gguf` is **8.10 GB** for the 7B (7.9 GB loaded in Ollama with a 4k context). 8-bit quantization is close to 16-bit quality, ensuring exact syntax and identifier retention.
- **Q4_K_M (4-bit Medium)**: `sql2pyspark-q4.gguf` is **4.68 GB** for the 7B, created via `llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-q4.gguf Q4_K_M`. Specifically tailored for CPU-only environments (such as the Ubuntu home server) where memory bandwidth and capacity are limited, roughly halving the memory read per token. On the eval sets it scored the same as or better than the Q8_0 build (differences within noise) and ran 4–33% faster on the M2; on the server's CPU it generates 1.64× as fast as v8's Q8_0 (4.05 vs. 2.47 tokens/s).

---

## 🦙 Ollama Packaging (`make ollama`, `make promote`)

The GGUF is registered in Ollama with the [Modelfile](../Modelfile):

```dockerfile
FROM ./sql2pyspark.gguf

TEMPLATE """{{ if .System }}<|im_start|>system
{{ .System }}<|im_end|>
{{ end }}{{ if .Prompt }}<|im_start|>user
{{ .Prompt }}<|im_end|>
{{ end }}<|im_start|>assistant
{{ .Response }}<|im_end|>"""

PARAMETER stop "<|im_end|>"
PARAMETER temperature 0.2
PARAMETER top_p 0.9

SYSTEM "You are an expert data engineer specializing in SQL and Apache Spark. Convert the provided SQL query into clean, idiomatic PySpark DataFrame code."
```

The system prompt matches the one used in the training data.

### Registration Commands

```bash
make export TAG=v9             # fuse + gguf + ollama create sql2pyspark:v9 -f Modelfile
make promote TAG=v10-q4 PREV=v9-q4   # keeps old latest as :v9-q4, then ollama cp sql2pyspark:v10-q4 sql2pyspark:latest
```

The Makefile defaults are `TAG=v9 PREV=v8` (`BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit`); v8 was exported from `complex_v8/adapters` by `eval/post_training_v8.sh` v9 from `complex_v9/adapters` by `eval/start_training_v9.sh`, and v10 from `classic_v10/adapters` by `eval/start_training_v10.sh`.

Current tags:

| Tag | Image ID / File | Notes |
| --- | --- | --- |
| `sql2pyspark:latest` | `096519684f69` | = v12-q4 on the Mac, promoted 2026-10-03 06:05 (v11-q4 2026-10-02 → 10-03, v9-q4 2026-10-01 → 10-02 and `latest` on the server until v12-q4 replaced it there on 2026-10-03 16:48) |
| `sql2pyspark:v8` | `aa01d9d717f7` | exported 2026-09-30 (7B, v7 continued on the complex_v8 data); rebuilt from its adapters 2026-09-30 (same ID); rollback for `latest` |
| `sql2pyspark:v7` | `d6a6a0edb9da` | exported 2026-09-28 23:14 (7B, continued from v6); `latest` 09-29 → 09-30; rollback: `ollama cp sql2pyspark:v7 sql2pyspark:latest` |
| `sql2pyspark:v9` | `72230e76c3d1` | exported 2026-10-01 from `sql2pyspark.gguf` (Q8_0, 8.1 GB); v8 continued on `complex_v9` (mirror window style) |
| `sql2pyspark:v9-q4` | `f19cf7096ebb` | the same weights as Q4_K_M (4.7 GB); production model (`latest`) since 2026-10-01 |
| `sql2pyspark:v10` | `1d7b0ee596e7` | exported 2026-10-01 18:44 from `sql2pyspark.gguf` (Q8_0, 8.1 GB); v9 continued on `classic_v10` |
| `sql2pyspark:v10-q4` | `d3f24a4893c2` | the same weights as Q4_K_M (4.7 GB); candidate, not promoted |
| `sql2pyspark:v11` | `98de659a4bb7` | exported 2026-10-02 12:01 from `sql2pyspark.gguf` (Q8_0, 8.1 GB); v10 continued on `targeted_v11` |
| `sql2pyspark:v11-q4` | `0afa9fb8d9b0` | the same weights as Q4_K_M (4.7 GB); `latest` on the Mac 2026-10-02 → 10-03 |
| `sql2pyspark:v12` | `3fb11c1936f0` | exported 2026-10-02 23:50 from `sql2pyspark.gguf` (Q8_0, 8.1 GB); v11 continued on `targeted_v12` |
| `sql2pyspark:v13` | `4052bde11540` | exported 2026-10-03 11:39 (Q8_0); a regression, not used |
| `sql2pyspark:v13-q4` | `409a47e29018` | Q4_K_M of v13; not used |
| `sql2pyspark:v14` | `9e585bcdede2` | exported 2026-10-07 00:50 (Q8_0); v12 continued on `targeted_v14`; not promoted |
| `sql2pyspark:v14-q4` | `87f0d5bd6909` | Q4_K_M of v14; not promoted |
| `sql2pyspark:v15` | `39e9aae00d19` | exported 2026-10-07 21:27 (Q8_0); v14 continued on `schema_v15`; promoted as `v15-q4` |
| `sql2pyspark:v15-q4` | `882787d22c3c` | Q4_K_M of v15 (`sql2pyspark-q4.gguf`); `latest` on the Mac since 2026-10-09 05:50, on the server since 06:29 |
| `sql2pyspark:v16` | `819c5cb3093b` | exported 2026-10-08 18:52 (Q8_0); v15 continued on `complex_v16`; not promoted |
| `sql2pyspark:v16-q4` | `0f940baf7d6a` | Q4_K_M of v16; not promoted |
| `sql2pyspark:v14-c2400-q4` | `2b18fa841a7c` | Q4_K_M of the v14 iter-2,400 checkpoint (`eval/eval_v14_ckpt.sh`); not promoted |
| `sql2pyspark:v12-q4` | `096519684f69` | the same weights as Q4_K_M (`sql2pyspark-q4.gguf`, 4.7 GB); `latest` on the Mac and the server 2026-10-03 → 10-09 |

The older tags were removed from the Mac's Ollama on 2026-09-30 to save ~24.6 GB: `v6` (`192e20ddb887`, 7B), `v5` (`b6a1ace642a4`, 3B), `v4` (`23108327a0d7`), `v3` (`aa95ad200568`), `v2` (`fb27933c2519`) and `v1` (`3dd2beaef9ff`, template-based). v2–v6 can be rebuilt from `sql2pyspark-mlx/runs/vN/adapters/` with `mlx_lm.fuse --adapter-path runs/vN/adapters` (v2–v5 with `--model mlx-community/Qwen2.5-Coder-3B-Instruct-4bit`), then `make gguf ollama TAG=vN` (v1's adapters were deleted with `archive/`).

`sql2pyspark.gguf` and `sql2pyspark-q4.gguf` hold the **v15** weights (`latest` on the Mac since 2026-10-09; exported 2026-10-07 21:27, overwritten by the v16 export on 2026-10-08 18:52 and restored from the Ollama blobs of `sql2pyspark:v15` / `:v15-q4`, same SHA-256; v12, `latest`, and v9 / v10 / v11 / v13 / v14 / v16 remain as Ollama tags; after the v13 and v14 exports the files had been restored to v12 the same way); Ollama keeps its own copy of every tag. `make fuse` also writes `sql2pyspark-hf/` and `sql2pyspark-mlx/sql2pyspark-model/`; `make clean` deletes them (and the LoRA checkpoints) once the export is in Ollama. The v2–v6 adapters, configs and logs are kept in `sql2pyspark-mlx/runs/v2/` … `runs/v6/`; v7's adapters are in `sql2pyspark-mlx/adapters/` and `runs/v7/`, v8's in `sql2pyspark-mlx/complex_v8/adapters/`, v9's in `sql2pyspark-mlx/complex_v9/adapters/`, and v10's in `sql2pyspark-mlx/classic_v10/adapters/`. To serve a model on another machine (e.g. the Ubuntu server behind sql2pyspark.praponb.com), copy `sql2pyspark.gguf` or `sql2pyspark-q4.gguf` and `Modelfile` there and run `ollama create sql2pyspark -f Modelfile`; see [Server Deployment](08-server-deployment.md).

The automated post-training script for v6 ([eval/post_training_v6.sh](../eval/post_training_v6.sh)) monitors `train.log`, triggers `make export TAG=v6`, runs all 4 evaluation sets with `--timeout 600` (since 7B generates ~2x slower than 3B), and reloads the Docker app service upon completion.

For v7, [eval/start_training_v7.sh](../eval/start_training_v7.sh) and [eval/post_training_v7.sh](../eval/post_training_v7.sh) provide unattended orchestration.

For v8, [eval/start_training_v8.sh](../eval/start_training_v8.sh) and [eval/post_training_v8.sh](../eval/post_training_v8.sh) automate the training of `complex_v8`.

For v9, [eval/start_training_v9.sh](../eval/start_training_v9.sh) orchestrates the entire workflow: verifies data prerequisites, stops background Docker and Ollama processes, fine-tunes with `complex_v9/lora_config.yaml` (2,400 steps resuming from v8 adapters), exports both Q8_0 (`sql2pyspark:v9`) and Q4_K_M (`sql2pyspark:v9-q4`), and benchmarks both models across all evaluation sets.

For v10, [eval/start_training_v10.sh](../eval/start_training_v10.sh) does the same with `classic_v10/lora_config.yaml` (resuming from v9's adapters), exports `sql2pyspark:v10` (Q8_0) and `sql2pyspark:v10-q4` (Q4_K_M), evaluates v10-q4 on the four eval sets and the same 100-query complex sample, and v10-q4 and v9-q4 on the 50 held-out `IN` / `NOT IN` pairs (`eval/results/inlist/`).

For v11, v12 and v13, [eval/start_training_v11.sh](../eval/start_training_v11.sh), [eval/start_training_v12.sh](../eval/start_training_v12.sh) and [eval/start_training_v13.sh](../eval/start_training_v13.sh) do the same with `targeted_v11/` and `targeted_v12/lora_config.yaml` (each resuming from the previous version's adapters) and add the held-out targeted pairs: `eval/results/v11-heldout/` (300), `eval/results/v12-heldout/` (250) and `eval/results/v13-heldout/` (150), scored for the new model and the earlier ones. [eval/start_training_v14.sh](../eval/start_training_v14.sh) does the same for v14 (`targeted_v14/lora_config.yaml`, resuming from v12's adapters; `eval/results/v14-heldout/`, 200), and [eval/eval_v14_ckpt.sh](../eval/eval_v14_ckpt.sh) exports and evaluates an earlier v14 checkpoint. [eval/start_training_v15.sh](../eval/start_training_v15.sh) does the same for v15 (`schema_v15/lora_config.yaml`, resuming from v14's adapters) and adds the schema-shift set (`eval/results/schema-shift/`) and the name-slip report. [eval/start_training_v16.sh](../eval/start_training_v16.sh) does the same for v16 (`complex_v16/lora_config.yaml`, resuming from v15's adapters) and adds the complex schema-shift set for v16-q4 / v15-q4 (`eval/results/complex-shift/`, a seeded 150 of 300).

The model is served at `POST http://localhost:11434/api/chat`.

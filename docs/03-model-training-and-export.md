# 03 - Model Training & Export Pipeline

This document covers fine-tuning, weight fusion, dequantization, GGUF conversion and Ollama packaging for the SQL → PySpark model.

---

## 🧠 Base Model Selection

The project has used two base models in the `Qwen2.5-Coder` family:

| Version | Base Model | Layers | Parameters | Context / Seq Len | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **v1–v5** | `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit` | 36 transformer blocks | 3.09 B | 3,200 tokens (v4/v5) | Fast training (~5.5 h) and fast serving (~3-4 s/query). Solves 95% of hold-out queries. |
| **v6** | `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` | 28 transformer blocks | 7.61 B | 3,200 tokens | Higher capacity for long, multi-CTE pipelines and complex nested expressions (v6: best on every eval set; still loops on `real001`). |

### Why scale from 3B to 7B in v6?
Across v1 through v5, 3B models achieved 95% accuracy on classic queries and 56% on v4 features, but scored 0/4 on real-world queries due to repetition loops (`real001` repeating window definitions until token exhaustion) and syntax slips under heavy nesting. While Kimi K3 repair fixes these in post-processing, a larger 7B base model provides the intrinsic representation capacity needed to generate dense, multi-stage pipelines end-to-end.

---

## ⚙️ LoRA Fine-Tuning Setup (`lora_config.yaml`)

LoRA (Low-Rank Adaptation) freezes the pretrained weights and trains small low-rank matrices added to the attention and MLP projections of the adapted layers.

Training is configured in [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) and run with `make train`, which calls [train_resumable.py](../sql2pyspark-mlx/train_resumable.py) (see [Crash-proof training](#crash-proof-training-train_resumablepy)).

### Current v6 Configuration (7B Base Model):

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

### Hyperparameter Evolution Across Versions:
1. **v2 / v3 (3B)**: `iters: 1200`, `batch_size: 4`, no accumulation, `max_seq_length: 1024`, no gradient checkpointing, peak ~9.7 GB, ~1 h 50 min.
2. **v4 (3B)**: `iters: 2400`, `batch_size: 2`, `grad_accumulation_steps: 2`, `max_seq_length: 3200`, gradient checkpointing enabled, peak 9.74 GB, ~5 h 16 min.
3. **v5 (3B)**: continued from v4 (`resume_adapter_file: ./runs/v4/adapters/adapters.safetensors`), half learning rate (5.0e-5), peak 10.31 GB, ~5 h 39 min.
4. **v6 (7B)**: `iters: 4800`, `batch_size: 1`, `grad_accumulation_steps: 4`, 8 of 28 layers adapted, gradient checkpointing. Dry run on the 64 longest samples showed `batch_size: 2` caused Metal OOM; `batch_size: 1` with accumulation 4 peaked at **9.0 GB** in the dry run and **10.1 GB** in the full run, within the ~12.7 GB Metal working set of a 16 GB Mac, at ~37–50 trained tokens/s (**8 h 19 min** in total, no crash).
5. **v7 (7B, continued from v6)**: `resume_adapter_file: ./runs/v6/adapters/adapters.safetensors`, `iters: 2400` (600 updates of 4 samples), `batch_size: 1`, `grad_accumulation_steps: 4`, peak LR 5.0e-5 (half of v6), warmup 50, cosine decay to 5.0e-6 over 550 updates. 4 h 58 min on a base M2, peak 9.9 GB, validation loss 0.011 → 0.021 (step 800) → 0.010 → **0.009**.
2. **Batch 1 × 4 accumulation + gradient checkpointing (7B since v6)**: on a 16 GB Mac, batch 2 runs out of Metal memory. Batch 1 with gradient accumulation 4 keeps peak memory at 9.0–10.1 GB.
3. **Masked prompt (`mask_prompt: true`)**: the system message and SQL prompt are excluded from the loss, so updates focus on generating the PySpark answer.
4. **Cosine decay with warmup**: v6 used 100 warmup updates to $10^{-4}$ decaying to $10^{-5}$ over 1,100 updates; v7 uses 50 warmup updates to $5 \times 10^{-5}$ decaying to $5 \times 10^{-6}$ over 550 updates.

### Crash-proof training (`train_resumable.py`)
The first v4 run crashed after ~250 iterations with `[METAL] Command buffer execution failed: Impacting Interactivity`: macOS aborts a GPU command buffer that runs too long, and mlx_lm cannot recover. `train_resumable.py`:
- runs `mlx_lm.lora` with smaller Metal command buffers (`MLX_MAX_OPS_PER_BUFFER=2`, `MLX_MAX_MB_PER_BUFFER=40`; ~9% slower), which avoided the error for the whole second run;
- if the run still dies, restarts from the newest checkpoint (saved every 100 iterations) for the remaining iterations, continuing the cosine learning-rate schedule from where it stopped (AdamW moments restart), up to 6 times.

---

## 📊 Training Dynamics (`train.log`)

### v7 (7B, continued from v6)
`sql2pyspark-mlx/train.log` is now the **v7** run (the v6 log is in `sql2pyspark-mlx/runs/v6/train.log`): `resume_adapter_file: ./runs/v6/adapters/adapters.safetensors`, peak learning rate 5e-5 (50 warmup updates, cosine to 5e-6), 600 updates of 4 samples on the v7 dataset as 2,400 micro-batches of 1. 18:13 → 23:11 (4 h 58 min) without a crash; peak memory 9.93 GB.

```
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

```
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

```
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

```
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

### Observations:
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

### Why Q8_0?
- **Accuracy**: 8-bit quantization is generally close to 16-bit quality, which matters for code, where one wrong token breaks execution. (Q8_0 vs. BF16 wasn't measured separately here; the evaluation scores are for the Q8_0 model.)
- **Size**: `sql2pyspark.gguf` is **8.10 GB** for the 7B (v6; 7.9 GB loaded in Ollama with a 4k context) and 3.29 GB for the 3B (v1–v5). On the M2 the 7B takes ~10 s per hold-out query and ~22 s per fixed-test query (3B: ~4 s and ~11 s). The whole v6 export (fuse, dequantize, convert, `ollama create`) took 2.5 minutes.

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

### Registration Commands:
```bash
make export TAG=v8             # fuse + gguf + ollama create sql2pyspark:v8 -f Modelfile
make promote TAG=v8 PREV=v7    # keeps old latest as :v7, then ollama cp sql2pyspark:v8 sql2pyspark:latest
```

The Makefile defaults are `TAG=v8 PREV=v7` (`BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit`).

Current tags:

| Tag | Image ID | Notes |
|---|---|---|
| `sql2pyspark:latest` | `d6a6a0edb9da` | = v7 (7B), promoted 2026-09-29 (see [Evaluation](04-evaluation-and-benchmarking.md#-v7-results-2026-09-29)) |
| `sql2pyspark:v7` | `d6a6a0edb9da` | exported 2026-09-28 23:14 (7B, continued from v6); current production model |
| `sql2pyspark:v6` | `192e20ddb887` | exported 2026-09-28 (7B, 8.1 GB); `latest` 09-28 → 09-29; rollback: `ollama cp sql2pyspark:v6 sql2pyspark:latest` |
| `sql2pyspark:v5` | `b6a1ace642a4` | exported 2026-09-27 (3B, continued from v4); previous `latest` (09-28); rollback: `ollama cp sql2pyspark:v5 sql2pyspark:latest` |
| `sql2pyspark:v4` | `23108327a0d7` | exported 2026-09-27; not promoted |
| `sql2pyspark:v3` | `aa95ad200568` | `latest` 2026-09-26 → 09-28 |
| `sql2pyspark:v2` | `fb27933c2519` | previous model; rollback: `ollama cp sql2pyspark:v2 sql2pyspark:latest` |
| `sql2pyspark:v1` | `3dd2beaef9ff` | template-based v1 |

`sql2pyspark.gguf`, `sql2pyspark-hf/` and `sql2pyspark-mlx/sql2pyspark-model/` hold the **v7** weights (last completed export); Ollama keeps its own copy of every tag. The v2–v6 adapters, datasets and logs are backed up in `sql2pyspark-mlx/runs/v2/` … `runs/v6/` (v7's adapters are in `sql2pyspark-mlx/adapters/`; v6 was trained on the v5 data); their GGUFs live inside Ollama. To serve a model on another machine (e.g. the Ubuntu server behind sql2pyspark.praponb.com), copy `sql2pyspark.gguf` and `Modelfile` there and run `ollama create sql2pyspark -f Modelfile`; see [Server Deployment](08-server-deployment.md).

The automated post-training script for v6 ([eval/post_training_v6.sh](../eval/post_training_v6.sh)) monitors `train.log`, triggers `make export TAG=v8`, runs all 4 evaluation sets with `--timeout 600` (since 7B generates ~2x slower than 3B), and reloads the Docker app service upon completion.

For v7, [eval/start_training_v7.sh](../eval/start_training_v7.sh) and [eval/post_training_v7.sh](../eval/post_training_v7.sh) provide unattended orchestration: waits for the v7 dataset build and v6 Kimi repair, terminates background Docker and Ollama models to free GPU memory, starts `make train` (2,400 steps, ~4.2 h), runs `make export TAG=v7`, benchmarks across all 4 evaluation sets with `--timeout 600`, reloads Docker, and executes the Kimi K3 repair loop on v7's real-query replies.

The model is served at `POST http://localhost:11434/api/chat`.

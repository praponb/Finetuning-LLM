# 03 - Model Training & Export Pipeline

This document covers fine-tuning, weight fusion, dequantization, GGUF conversion and Ollama packaging for the SQL → PySpark model.

---

## 🧠 Base Model Selection

- **Model**: `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit`
- **Architecture**: `Qwen2ForCausalLM` (36 transformer layers, hidden size 2048, 16 attention heads / 2 KV heads, 151,936-token vocabulary)
- **Parameters**: 3.09 B (trainable LoRA parameters: 6.65 M = 0.216%)
- **Precision**: 4-bit quantized MLX base weights

### Why Qwen2.5-Coder-3B?
The very first experiment (formerly `archive/my-finetuned-model/`, deleted 2026-09-27) fine-tuned the general `Qwen2.5-3B-Instruct`. It was never evaluated. From v1 onward the project used the code-specialized `Qwen2.5-Coder-3B-Instruct`: it is the same size, fits the 16 GB training budget, and is pretrained on code. The two base models were not compared head to head.

---

## ⚙️ LoRA Fine-Tuning Setup (`lora_config.yaml`)

LoRA (Low-Rank Adaptation) freezes the pretrained weights and trains small low-rank matrices added to the attention and MLP projections of the adapted layers.

Training is configured in [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) and run with `make train`, which calls [train_resumable.py](../sql2pyspark-mlx/train_resumable.py) (see [Crash-proof training](#crash-proof-training-train_resumablepy)). The v4 configuration:

```yaml
model: mlx-community/Qwen2.5-Coder-3B-Instruct-4bit
train: true
fine_tune_type: lora
data: ./data
adapter_path: ./adapters
seed: 0

iters: 2400                  # micro-batches: 1,200 optimizer updates x 4 samples, as batch 2 x 2 accumulation
batch_size: 2
grad_accumulation_steps: 2
max_seq_length: 3200         # longest v4 training sample: 3,119 tokens (v3 data: max 603)
mask_prompt: true            # loss only on the assistant (PySpark) tokens
grad_checkpoint: true        # needed for v4's long samples (~40% slower); v2/v3 ran without it

num_layers: 8                # adapt the last 8 of 36 transformer blocks (16 was ~1.4x slower per step)
lora_parameters:
  rank: 16
  dropout: 0.05
  scale: 20.0

optimizer: adamw
learning_rate: 1.0e-4
lr_schedule:
  name: cosine_decay
  warmup: 100
  warmup_init: 1.0e-6
  arguments: [1.0e-4, 1100, 1.0e-5]   # [init, decay_steps, end]  (counted in optimizer updates)

steps_per_report: 50
steps_per_eval: 600
val_batches: 50              # 100 held-out-signature samples per eval
save_every: 100              # frequent checkpoints so train_resumable.py loses little after a crash
```

v2 and v3 used `iters: 1200`, `batch_size: 4`, no accumulation, `max_seq_length: 1024`, no gradient checkpointing, and evaluated/saved every 300 steps; the backups in `sql2pyspark-mlx/runs/v2/` and `runs/v3/` contain their configs.

### Key Hyperparameter Decisions:
1. **8 adapted layers, 1,200 optimizer updates of 4 samples**: the original plan was 16 layers and ~3,000 steps, but on a base M2 that ran at 0.138 it/s (~6.5 h). With 8 layers and the short v2/v3 samples it ran at ~0.19 it/s, and the data is learned quickly, so 1,200 updates (~1 h 50 min – 2 h) was chosen. v4 keeps the same number of updates and samples (~0.28 epoch of 16.9k pairs).
2. **Batch 2 × 2 accumulation + gradient checkpointing (v4 only)**: v4's pipelines reach 3.1k tokens. A dry run on the 64 longest training samples ran out of GPU memory with batch 4 (with or without checkpointing) and with batch 2 without checkpointing; batch 2 with checkpointing peaked at 9.8 GB. A 16 GB Mac lets Metal use ~10.7–12.7 GB, so this is the largest setting that fits. Note that `iters` counts micro-batches, while the learning-rate schedule counts optimizer updates.
3. **Masked prompt (`mask_prompt: true`)**: the system message and SQL prompt are excluded from the loss, so updates focus on generating the PySpark answer.
4. **Cosine decay with warmup**: 100 warmup updates from $10^{-6}$ to $10^{-4}$, then decaying to $10^{-5}$ over 1,100 updates.

### Crash-proof training (`train_resumable.py`)
The first v4 run crashed after ~250 iterations with `[METAL] Command buffer execution failed: Impacting Interactivity`: macOS aborts a GPU command buffer that runs too long, and mlx_lm cannot recover. `train_resumable.py`:
- runs `mlx_lm.lora` with smaller Metal command buffers (`MLX_MAX_OPS_PER_BUFFER=2`, `MLX_MAX_MB_PER_BUFFER=40`; ~9% slower), which avoided the error for the whole second run;
- if the run still dies, restarts from the newest checkpoint (saved every 100 iterations) for the remaining iterations, continuing the cosine learning-rate schedule from where it stopped (AdamW moments restart), up to 6 times.

---

## 📊 Training Dynamics (`train.log`)

### v5 (continued from v4)
`sql2pyspark-mlx/train.log` is now the **v5** run: `resume_adapter_file: ./runs/v4/adapters/adapters.safetensors`, peak learning rate 5e-5 (50 warmup updates, cosine to 5e-6), 1,200 updates of 4 samples on the v5 mix (55% classic, 25% short v4 features, 20% pipelines). 17:17 → 22:56 (5 h 39 min) without a crash; peak memory 10.31 GB.

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
Saved in `sql2pyspark-hf/` (BF16, ~6.2 GB):
- `model-00001-of-00002.safetensors` (5.36 GB)
- `model-00002-of-00002.safetensors` (816 MB)
- `tokenizer.json`, `config.json`, `chat_template.jinja`, `model.safetensors.index.json`

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
- **Size**: `sql2pyspark.gguf` is 3.29 GB, the same size as v1, and runs at about 3–6 s per query in Ollama on the M2 (v3: 3.3 s on the hold-out set, 5.3 s on the test sample).

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
make export TAG=v4             # fuse + gguf + ollama create sql2pyspark:v4 -f Modelfile
make promote TAG=v4 PREV=v3    # keeps the old latest as :v3 (if that tag is missing), then ollama cp sql2pyspark:v4 sql2pyspark:latest
```

The Makefile defaults are `TAG=v4 PREV=v3`.

Current tags:

| Tag | Image ID | Notes |
|---|---|---|
| `sql2pyspark:latest` | `aa95ad200568` | = v3, promoted 2026-09-26 |
| `sql2pyspark:v5` | `b6a1ace642a4` | exported 2026-09-27 (continued from v4); not promoted (see [Evaluation](04-evaluation-and-benchmarking.md)) |
| `sql2pyspark:v4` | `23108327a0d7` | exported 2026-09-27; not promoted |
| `sql2pyspark:v3` | `aa95ad200568` | current model |
| `sql2pyspark:v2` | `fb27933c2519` | previous model; rollback: `ollama cp sql2pyspark:v2 sql2pyspark:latest` |
| `sql2pyspark:v1` | `3dd2beaef9ff` | template-based v1 |

`sql2pyspark.gguf`, `sql2pyspark-hf/` and `sql2pyspark-mlx/sql2pyspark-model/` hold the **v5** weights (last export); Ollama keeps its own copy of every tag, so `latest` still serves v3. The v2, v3 and v4 adapters, datasets and logs are backed up in `sql2pyspark-mlx/runs/v2/`, `runs/v3/` and `runs/v4/`; their GGUFs live only inside Ollama.

The model is served at `POST http://localhost:11434/api/chat`.

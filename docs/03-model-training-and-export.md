# 03 - Model Training & Export Pipeline

This document covers fine-tuning, weight fusion, dequantization, GGUF conversion and Ollama packaging for the SQL → PySpark model.

---

## 🧠 Base Model Selection

- **Model**: `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit`
- **Architecture**: `Qwen2ForCausalLM` (36 transformer layers, hidden size 2048, 16 attention heads / 2 KV heads, 151,936-token vocabulary)
- **Parameters**: 3.09 B (trainable LoRA parameters: 6.65 M = 0.216%)
- **Precision**: 4-bit quantized MLX base weights

### Why Qwen2.5-Coder-3B?
The very first experiment (`archive/my-finetuned-model/`) fine-tuned the general `Qwen2.5-3B-Instruct`. It was never evaluated. From v1 onward the project used the code-specialized `Qwen2.5-Coder-3B-Instruct`: it is the same size, fits the 16 GB training budget, and is pretrained on code. The two base models were not compared head to head.

---

## ⚙️ LoRA Fine-Tuning Setup (`lora_config.yaml`)

LoRA (Low-Rank Adaptation) freezes the pretrained weights and trains small low-rank matrices added to the attention and MLP projections of the adapted layers.

Training is configured in [lora_config.yaml](../sql2pyspark-mlx/lora_config.yaml) and run with `make train`:

```yaml
model: mlx-community/Qwen2.5-Coder-3B-Instruct-4bit
train: true
fine_tune_type: lora
data: ./data
adapter_path: ./adapters
seed: 0

iters: 1200                  # ~0.4 epoch over ~12.7k training pairs at batch 4
batch_size: 4
grad_accumulation_steps: 1
max_seq_length: 1024
mask_prompt: true            # loss only on the assistant (PySpark) tokens
grad_checkpoint: false       # true cut peak memory 13 GB -> 4.5 GB (16-layer run) but was ~40% slower

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
  arguments: [1.0e-4, 1100, 1.0e-5]   # [init, decay_steps, end]

steps_per_report: 25
steps_per_eval: 300
val_batches: 25              # 100 held-out-signature samples per eval
save_every: 300
```

### Key Hyperparameter Decisions:
1. **8 adapted layers, 1,200 steps**: the original plan was 16 layers and ~3,000 steps, but on a base M2 that ran at 0.138 it/s (~6.5 h). With 8 layers it runs at ~0.19 it/s, and the data is learned quickly (train loss 0.053 after 50 steps), so 1,200 steps (~1 h 50 min) was chosen.
2. **No gradient checkpointing**: tried it to save memory; it cut peak memory a lot but made each step ~40% slower. Memory was not the bottleneck, compute was.
3. **Masked prompt (`mask_prompt: true`)**: the system message and SQL prompt are excluded from the loss, so updates focus on generating the PySpark answer.
4. **Cosine decay with warmup**: 100 warmup steps from $10^{-6}$ to $10^{-4}$, then decaying to $10^{-5}$ over 1,100 steps.

---

## 📊 Training Dynamics (`train.log`)

Measured on a base Apple M2 (16 GB):

```
Step    Val Loss    Train Loss    Learning Rate    Speed (it/s)    Trained Tokens    Peak Mem
─────────────────────────────────────────────────────────────────────────────────────────────
1       0.605       —             —                —               —                 —
25      —           0.197         2.476e-5         0.189           11,015            7.05 GB
50      —           0.053         4.951e-5         0.190           21,978            8.77 GB
100     —           0.054         9.901e-5         0.189           44,306            9.22 GB
300     0.011       0.014         9.299e-5         0.196           130,933           9.68 GB
600     0.007       0.008         6.166e-5         0.187           261,780           9.68 GB
900     0.006       0.005         2.573e-5         0.197           391,073           9.68 GB
1200    0.004       0.005         1.000e-5         0.183           522,702           9.68 GB
```

### Observations:
- **Fast convergence**: training loss fell below 0.06 within 50 steps; the generated data is very regular.
- **Held-out signatures**: validation loss (on feature combinations never seen in training) fell steadily to **0.004**, and stayed close to training loss, so there is no sign of overfitting. The hand-written hold-out set (90% correct) is the stronger generalisation test; see [Evaluation](04-evaluation-and-benchmarking.md).
- **Memory**: peak 9.68 GB. That fits a 16 GB Mac, but running Ollama inference at the same time caused a Metal out-of-memory crash, so evaluations were run after training.
- **Duration**: ~1 h 50 min for 1,200 steps (plus the first attempts, which were stopped and restarted).

Checkpoints are saved every 300 steps in `sql2pyspark-mlx/adapters/`:
- `0000300_adapters.safetensors`
- `0000600_adapters.safetensors`
- `0000900_adapters.safetensors`
- `0001200_adapters.safetensors` (same as the final `adapters.safetensors`)

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
- **Size**: `sql2pyspark.gguf` is 3.29 GB, the same size as v1, and runs at about 4–5 s per query in Ollama on the M2.

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
make ollama TAG=v2     # ollama create sql2pyspark:v2 -f Modelfile
make promote TAG=v2    # keeps the old latest as :v1, then ollama cp sql2pyspark:v2 sql2pyspark:latest
```

Current tags: `sql2pyspark:latest` = `sql2pyspark:v2` (ID `fb27933c2519`), and `sql2pyspark:v1` (`3dd2beaef9ff`).

The model is served at `POST http://localhost:11434/api/chat`.

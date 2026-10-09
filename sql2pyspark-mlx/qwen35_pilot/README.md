# qwen35_pilot — can Qwen3.5-9B be trained, exported and served on this setup? (feasibility pilot, 2026-10-09)

Qwen3.8's smallest model is 27B (4-bit MLX 16.1 GB): too big for the 16 GB M2 and the 13 GB server. The closest
newer base that fits is **Qwen3.5-9B** (`mlx-community/Qwen3.5-9B-4bit`, 6.0 GB): a hybrid model where 3 of every 4
layers are linear attention (Gated DeltaNet) and every 4th is full attention, with a 248k vocabulary. This pilot
answers "can we train, export and serve it?" before anyone spends days on a full run. It does **not** compare quality:
400 iterations are 1/12 of a v15-sized run.

Nothing here touches `sql2pyspark:latest` / `v15-q4`, the root `sql2pyspark*.gguf` files or the server.

## Verdict

**No-go for a full run as planned** — two of the four go/no-go checks fail:

| Check (plan) | Result | |
| --- | --- | --- |
| Peak memory ≤ 12.5 GB with max_seq ≥ 2600 and ≥ 6 layers | **12.62 GB** at max_seq 2600, 8 layers — and only with three local patches to mlx-lm (below); stock mlx-lm 0.31.3 runs out of memory even at 2600 | fail (borderline) |
| Full run (4,800 iterations) ≤ ~8 h | **~14 h** (~10.5 s / iteration, v15 on Qwen2.5-7B: 8.1 h) | fail |
| Export works, no `<think>` leak | works (fuse → norm fix → GGUF → Q4_K_M → Ollama, ~7 min), no leak — but Ollama answers in `message.thinking` unless the request sends `think: false` | pass (app change needed) |
| Predicted server speed ≥ ~3 tok/s | **3.25 tok/s** (Mac CPU-only 11.7 vs v15-q4 14.4 tok/s) | pass (narrow) |

Hold-out after 400 iterations: **41 / 62** (v15-q4: 60 / 62 after 4,800). Untuned bases on the same hold-out:
Qwen3.5-9B **0 / 62** (3 with the lenient scoring), Qwen2.5-Coder-7B **8 / 62** (the first zero-shot numbers for this
project) — so 400 iterations already take Qwen3.5 from 0 to 41.

If a Qwen3.5 run is still wanted, the realistic shapes are (a) the full 4,800 iterations over two nights with
`train_resumable.py` resuming in between, or (b) ~2,400 iterations (~7 h) and accept fewer samples per pair; either
way it depends on the local patches below until mlx-lm trains Gated DeltaNet layers efficiently itself.

## What happened, step by step

| Step | Result |
| --- | --- |
| 1. Load + template ([`step1_check.py`](step1_check.py), [`step1.log`](step1.log)) | Loads in mlx-lm 0.31.3. The training render (`apply_chat_template`, no arguments) equals the non-thinking prompt (`<\|im_start\|>assistant\n<think>\n\n</think>\n\n`) + the code; mlx_lm.lora's prompt mask ends at `<think>\n`, so the loss also covers `\n</think>\n\n`. Untuned replies: chatty, bare `patients` instead of `df_patients`, no think leak. |
| 2. Memory ([`memtest.yaml`](memtest.yaml), 64 longest samples) | Stock mlx-lm: Metal OOM at max_seq 3200 **and** 2600. With the patches below: 9.32 GB at 3200 — but steps of ~3,000 tokens trip the macOS GPU watchdog, so the run uses max_seq 2600 and drops the 246 longer samples (1.0%, [`filter_mix.py`](filter_mix.py)). |
| 3. 400 iterations ([`lora_config.yaml`](lora_config.yaml), [`train.log`](train.log)) | Fresh adapters, peak lr 1e-4, last 8 of 32 layers, rank 16. Val loss 0.299 → 0.120 (iteration 150) → 0.078 (400). 10.4 s / iteration (30 tokens/s), peak 12.62 GB. One Metal OOM after iteration 150, then two watchdog aborts on the first step of the resume, see below (plus three in the first attempt with `gd_patch.py`, [`train_attempt1_gdpatch.log`](train_attempt1_gdpatch.log)). |
| 4. Export ([`chain.sh`](chain.sh)) | `mlx_lm.fuse --dequantize` (17 GB bf16, 1 min) → [`fix_norms.py`](fix_norms.py) → `convert_hf_to_gguf.py --no-mtp` (f16, 17.9 GB) → `llama-quantize Q4_K_M` (5.63 GB; stock unsloth Q4_K_M 5.68 GB) → `ollama create sql2pyspark:q35-pilot-q4` ([`Modelfile.q35`](Modelfile.q35)). Ollama 0.35.1 loads `qwen35` (5.9 GB, 100% GPU). |
| 5. Smoke + hold-out ([`results/`](results/)) | 41 / 62 (strict = lenient), 6.0 s / query on the GPU. Failures are undertraining (invented `F.distinct`, `.over(F.groupBy(...))`, a join instead of a semi-join); v15-q4 gets 19 of the 21. |
| 6. CPU-only speed ([`speed_test.py`](speed_test.py), `num_gpu 0`) | v15-q4 14.37 tok/s, pilot 11.70 tok/s (−19%); prompt 85.9 vs 56.3 tok/s (−34%); cold load 6.9 vs 7.1 s. Server prediction 4.0 × 11.70 / 14.37 = **3.25 tok/s**. |
| + Untuned bases | Stock GGUFs (unsloth `Qwen3.5-9B-Q4_K_M`, Qwen `qwen2.5-coder-7b-instruct-q4_k_m`) with the project's system prompt: Qwen3.5-9B 0 / 62 strict, 3 lenient (28.4 s / query: long explanations, bare table names like `patients`, `F.window` for windows); Qwen2.5-Coder-7B 8 / 62 (12.6 s / query). |

## The local patches (mlx-lm 0.31.3)

Started through [`lora_q35.py`](lora_q35.py) (`train_resumable.py --launcher qwen35_pilot/lora_q35.py`):

- **[`gd_chunked.py`](gd_chunked.py)** — in training mode mlx-lm runs the gated delta rule as a Python loop over the
  tokens (`gated_delta_ops`): ~10 tiny ops per token per layer and ~8 MB of saved fp32 states per token for the
  backward pass (~20 GB at 2,600 tokens). This replaces it with the chunked (WY) form of transformers'
  `torch_chunk_gated_delta_rule` in MLX ops: 64-token chunks become matrix products, one state per chunk is carried, and
  autodiff gives a matmul backward. Outputs and gradients match mlx-lm's reference (≤ 0.5% relative, bf16 inputs).
  ([`gd_patch.py`](gd_patch.py) is the first attempt — Metal-kernel forward + chunked recompute backward via
  `mx.depends`: memory-bounded, but compiled steps tripped the watchdog and it was ~25% slower.)
- **[`chunked_loss.py`](chunked_loss.py)** — lm_head + masked cross-entropy in checkpointed 256-position chunks, so the
  248k-wide logits never exist for the whole sequence (3,200 × 248,320 = 1.6 GB in bf16, plus softmax and gradient).
  Same value as `default_loss`; no `.item()` / `.tolist()` because mlx-lm compiles the step.
- **MLX memory** — 512 MB free-buffer cache and a 12 GB memory limit. With a 1 GB cache a step ran out of Metal memory
  (active peak 12.25 GB + cache); with no cache the first step after a resume tripped the watchdog twice.

What the watchdog (`kIOGPUCommandBufferCallbackErrorImpactingInteractivity`) did during the pilot: it fired with the
display off too; small command buffers (`MLX_MAX_OPS_PER_BUFFER=2`, as the Qwen2.5 runs) made it worse with the per-token
loop; [`step_replay.py`](step_replay.py) showed compiled per-token steps abort while uncompiled ones pass. With
`gd_chunked` + default buffers + the 512 MB cache the last 250 iterations ran without an abort.

Also fixed on the way, outside this folder: `train_resumable.py` crashed with `shutil.SameFileError` when the first
segment died after a checkpoint (it copied the checkpoint onto itself), so it never resumed; and it gained
`--launcher`. `eval/evaluate.py` gained `--think true|false` (default: not sent).

## Export notes for a real run

- mlx-lm stores the Qwen3.5 RMSNorm weights shifted by +1 (its `sanitize()`), and `mlx_lm.fuse` saves them that way;
  llama.cpp's converter adds 1 itself. [`fix_norms.py`](fix_norms.py) subtracts 1 from the 81 `input_layernorm`,
  `post_attention_layernorm`, `q_norm`, `k_norm` and final norm tensors (not `linear_attn.norm`). Check: the final norm
  mean is 2.140 in the MLX weights and 2.1398 in the stock GGUF.
- The 4-bit MLX model has no MTP head: convert with `--no-mtp`.
- Ollama renders `qwen35` with its built-in Qwen3.5 template and parser; the Modelfile `TEMPLATE` is not used, and
  without `think: false` the reply lands in `message.thinking`. `app.py` would have to send `think: false` (the evals
  here do, via `--think false`).

## Files

Scripts, configs and logs are tracked; the data copies (`data_long/`, `data_mix2600/`, rebuilt by
[`make_long_set.py`](make_long_set.py) / [`filter_mix.py`](filter_mix.py)), the intermediate checkpoints and `gguf/`
are not. Kept on disk: `adapters/adapters.safetensors` (final), `gguf/pilot-q4.gguf`, Ollama tags
`sql2pyspark:q35-pilot-q4`, `sql2pyspark:base-q35-q4`, `sql2pyspark:base-coder7b-q4`.

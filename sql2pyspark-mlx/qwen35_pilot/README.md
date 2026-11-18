# qwen35_pilot — Qwen3.5-9B: feasibility pilot (2026-10-09), runs 2-9 (2026-11-09 → 11-12) and the fresh run q9a (2026-11-17/18)

Qwen3.8's smallest model is 27B (4-bit MLX 16.1 GB): too big for the 16 GB M2 and the 13 GB server. The closest
newer base that fits is **Qwen3.5-9B** (`mlx-community/Qwen3.5-9B-4bit`, 6.0 GB): a hybrid model where 3 of every 4
layers are linear attention (Gated DeltaNet) and every 4th is full attention, with a 248k vocabulary. This pilot
answers "can we train, export and serve it?" before anyone spends days on a full run. It does **not** compare quality:
400 iterations are 1/12 of a v15-sized run.

Nothing here touches `sql2pyspark:latest` (v18a-q4 since 2026-11-15; v15-q4 before), the root `sql2pyspark*.gguf` files or the site.

## Run q9a (2026-11-17/18): fresh QLoRA from the base — best Qwen3.5 so far, not promoted

After the Qwen3.5-35B-A3B rounds (see [qwen35moe](../qwen35moe/README.md)) were discarded, the owner switched back to
Qwen3.5-9B + QLoRA (LoRA on the 4-bit base `mlx-community/Qwen3.5-9B-4bit`) and asked for a **fresh start from the
base**. Reason: runs 3-9 trained with a `train_resumable.py` bug — every resumed segment reused the same seed, so after
each Metal restart training replayed the start of the same batch order and much of the data was never seen. Fixed on
2026-11-15 (`seed + 1000 * segment`).

- Config [`lora_config_q9a.yaml`](lora_config_q9a.yaml): run 6's adapter shape (last 16 of 32 layers, rank 32,
  scale 10, dropout 0.05), batch 1 × grad-accum 4, max_seq 3200, AdamW lr 5e-5 (warmup 50 → cosine to 5e-6 over
  2,350 updates), 9,600 micro-batches = 2,400 updates, seed 43.
- Data `data_q9a/`: the whole v17 / v18a mix (29,008) with 8,000 pairs renamed (`namecopy/build.py --keep all
  --seed 2028`); valid 437.
- Chain [`chain_q9a.sh`](chain_q9a.sh) (logs `chain_q9a.log`, `train_q9a.log`): training with a divergence guard
  (stops if the latest val loss > 3× the best and > 0.05) → export (fuse `--dequantize` → `fix_norms.py` → f16 GGUF
  `--no-mtp` → Q4_K_M, `gguf/q9a-q4.gguf` 5.2 GB) → Ollama `sql2pyspark:q9a-q4` → the 12 eval sets → the 74
  real-style pairs → name slips vs. v18a-q4.
- Training 2026-11-17 14:07 → 11-18 03:52 Bangkok (~13.7 h, 13 automatic `metal::malloc` restarts, peak 15.9 GB, no
  swap); val loss 0.189 → 0.015 (it 1,200) → 0.006 (it 6,400) → **0.004** (it 9,600), the lowest of any Qwen3.5 run.
  Evals 03:53 → 09:37.

| Eval set | **q9a-q4** | v18a-q4 (`latest`) | run 8 (`q35m6g-q4`) |
| --- | --- | --- | --- |
| Hold-out (62) | 59 | **61** | 59 |
| v4 hold-out (25) | 20 | 20 | **22** |
| Real queries (4) | 0 | 0 | 0 |
| Fixed test (150) | **145** | **145** | 143 |
| complex_v8 test (100) | **93** | 88 | 82 |
| `IN` / `NOT IN` (50) | **50** | **50** | |
| targeted_v11 / v12 held-out (300 / 250) | 279 / **236** | **281** / 232 | |
| targeted_v13 / v14 held-out (150 / 200) | 143 / **197** | **148** / 195 | |
| Schema-shift (200) | 184 | **186** | |
| Complex schema-shift (150 of 300) | 122 | **126** | 111 |
| Real-style pairs (74) | 0 | 0 | |
| Failures / with a name slip (all sets) | 183 / 83 | **179** / **66** | |

**Verdict: not promoted** (owner 2026-11-18: keep v18a-q4). q9a has the best complex score of any model (93 vs. 88) and
wins v12 / v14 held-out, but is below v18a-q4 on hold-out, v11, v13, schema-shift and complex-shift, so the promotion
rule fails. Its weak spot is names on unseen schemas: 25 of its 28 complex-shift failures are name slips (v18a 10). On
v13 it collects a scalar subquery into a Python number three times (`'float' object has no attribute 'alias'`).
Hold-out misses: `holdout031` (v18a fails too), `holdout032` (drops the `-` of a `REPLACE`), `holdout058` (a join
that multiplies rows, 180 vs. 20). Kept on the Mac mini: `adapters_q9a/` (final + checkpoints, 2.4 GB; the 173 MB
final adapter is over GitHub's 100 MB limit, so only `adapter_config.json` is tracked), `gguf/q9a-q4.gguf`, the tag
`sql2pyspark:q9a-q4`.

## Mac mini M6 runs (2026-11-09 → 11-12): runs 2-9 — stopped, Qwen2.5-Coder-7B stays

On the 32 GB Mac mini the memory and speed blockers below are gone (patched peak 14-16 GB, ~80-106 tokens/s), so the
owner asked for real runs. Eight runs followed, each continuing the previous adapter; every chain script
(`chain_m6*.sh`) trains via `train_resumable.py --launcher lora_q35.py`, exports (fuse `--dequantize` → `fix_norms.py`
→ f16 GGUF `--no-mtp` → Q4_K_M) to a scratch Ollama tag and runs 5 evals with `--think false`. None of them touched
`sql2pyspark:latest` or the site.

| Run | Config | What changed | Iterations | hold-out /62 | v4 /25 | fixed /150 | complex /100 | complex-shift /150 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 (pilot 2) | `lora_config_m6.yaml` | pilot adapters + 1,600 it on the v17 mix, max_seq 3200, lr 1e-4 | 2,000 | 51 | 13 | 111 | 38 | 51 |
| 3 | `lora_config_m6b.yaml` | + 4,800 it, lr 1e-4 → 1e-5 | 6,800 | 44 | 15 | 115 | 55 | 59 |
| 4 | `lora_config_m6c.yaml` | + 4,800 it, lr 3e-5 → 3e-6 | 11,600 | 46 | 17 | 128 | 64 | 74 |
| 5 | `lora_config_m6d.yaml` | + 2,400 it on `data_namecopy` (name copying, below), lr 3e-5 | 14,000 | 52 | 16 | 134 | 65 | 74 |
| 6 | `lora_config_m6e.yaml` | run 5 fused into the base (`base_r5_fused`); **new 16-layer rank-32 adapter** (43.3M parameters), scale 10, lr 2e-5, `data_m6e` | 4,800 | 59 | 21 | 136 | 77 | 94 |
| 7 | `lora_config_m6f.yaml` | run 6 + 4,800 it, lr 1e-5 | 9,600 | 57 | 22 | 143 | 81 | 101 |
| **8** | `lora_config_m6g.yaml` | run 7 + 2,400 it on a fresh name-copy set `data_m6g`, lr 5e-6 | 12,000 | **59** | **22** | **143** | **82** | **111** |
| 9 | `lora_config_m6h.yaml` | run 8 + 2,400 it on the complex-focused `data_m6h`, lr 5e-6 | 14,400 | 59 | 22 | 141 | 81 | 111 |
| v15-q4 (`latest`) | | Qwen2.5-Coder-7B | | 60 | 20 | 145 | 89 | 110 |
| v17-q4 | | Qwen2.5-Coder-7B | | 61 | 21 | 142 | 85 | 116 |

**Verdict (2026-11-12): stopped.** The best Qwen3.5 model, run 8 (`sql2pyspark:q35m6g-q4`), ties the Qwen2.5-Coder-7B
models on hold-out, v4 and complex-shift but stays 7 below v15-q4 on the complex test, and run 9's 5,000 unseen complex
pairs did not move it (82 → 81). Complex queries also take ~1.4× longer (~26 s vs. ~18 s). Qwen2.5-Coder-7B stays the
base; v15-q4 stays `latest`.

What the runs showed:

- **Name drift**: runs 3-4 corrupted table names on the hold-out (`df_doctors` → `df_drators`, `df_patients` →
  `df_patents`), the same with MLX inference, so it came from training, not the export. **Name copying** fixed most of
  it: [`namecopy/build.py`](namecopy/build.py) renames the base tables of 8,000 mix pairs consistently in SQL and code
  (`df_X` → `df_Y`, 6,900+ fresh names, none equal to any eval identifier). Replies with a table name not in the SQL, over the 5 eval sets: run 3 44, run 4 37, run 5
  18, runs 6-9 1-6. Options `--out`, `--keep N|all`, `--seed` (run 5: defaults; run 6: `--keep all --out data_m6e`;
  run 8: `--seed 2027 --out data_m6g`).
- **Adapter size** was the main lever for hard queries: 8 layers × rank 16 plateaued at complex 64-65; 16 layers ×
  rank 32 reached 77 in one run. Scale 20 with lr 5e-5 diverged (first attempt of run 6, val 0.014 → 0.039,
  `*_attempt1.log`); scale 10 with lr 2e-5 trained cleanly.
- **More complex data did not help**: [`complexmix/build.py`](complexmix/build.py) (run 9: 5,000 unseen complex_v9 +
  2,000 unseen complex_v16 + 2,000 renamed + 3,000 replay; 0 of the 837 eval queries inside) lowered validation loss to
  0.005 but left complex at 81.
- **Training on the M6**: ~80 tokens/s for the rank-32 adapter, peak ~15.7 GB. Every ~400-1,000 iterations MLX hit
  `[metal::malloc] Resource limit (499000) exceeded` (a Metal buffer-count limit, not memory); `train_resumable.py`
  resumed from the last checkpoint each time (up to 8 restarts per run; a restart resets the AdamW state).

Kept on the Mac mini (git-ignored): `base_r5_fused/` (4.7 GB, `mlx_lm.fuse --model mlx-community/Qwen3.5-9B-4bit
--adapter-path adapters_m6d`, no `--dequantize`; needed to export or continue runs 6-9), the rank-32 adapters of runs
6-9 (169 MB each, over GitHub's 100 MB limit), `gguf/m6g-q4.gguf` and the Ollama tag `sql2pyspark:q35m6g-q4`. The other
run tags and GGUFs were removed on 2026-11-12. Qwen3.8 (2026-11-09) only exists as 27B dense and larger: its pilot ran
out of Metal memory on long samples at ~50 tokens/s (peak 25.8 GB), so it was not pursued.

## The original pilot (2026-10-09, M2 16 GB)

### Verdict

**No-go for a full run as planned** — two of the four go/no-go checks fail. **Decision (2026-10-10): not used for now**;
this folder, the adapters, `gguf/pilot-q4.gguf` and the Ollama tags are kept for a later run on a machine with more memory.

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

Runs 2-9 (Mac mini): `lora_config_m6{,b,c,d,e,f,g,h}.yaml`, `chain_m6*.sh` (logs `chain_m6*.log`, `train_m6*.log`),
`Modelfile.q35m6*`, `results/q35m6*-q4__*.json`, the data builders `namecopy/build.py` and `complexmix/build.py` and
their outputs `data_namecopy/`, `data_m6e/`, `data_m6g/`, `data_m6h/` (tracked), the final adapters `adapters_m6*/`
(runs 2-5 tracked; runs 6-9's 169 MB files only on the Mac mini, with `base_r5_fused/`).

Run q9a: `lora_config_q9a.yaml`, `chain_q9a.sh` (logs `chain_q9a.log`, `train_q9a.log`, `quantize_q9a.log`),
`Modelfile.q9a`, `data_q9a/`, `adapters_q9a/adapter_config.json` (tracked); results in the repo's `eval/results/`
(`q9a-q4__*.json` and the sub-folders, like v18a).

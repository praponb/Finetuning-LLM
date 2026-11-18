# qwen35moe — Qwen3.5-35B-A3B (2026-11-15 → 11-17): discarded

On 2026-11-15 the owner stopped Qwen2.5 training and asked to try `mlx-community/Qwen3.5-35B-A3B-4bit` (a mixture of
experts: 40 layers, 256 routed experts top-8 + a shared expert, 3 linear-attention layers per full-attention layer,
248k vocabulary; 20.4 GB 4-bit). Three rounds were trained on the Mac mini M6 (32 GB). None beat v18a-q4, the third
diverged, and on 2026-11-17 the owner **discarded the model** (Ollama tags, GGUFs, adapters and the download deleted)
and switched to Qwen3.5-9B + QLoRA ([qwen35_pilot, run q9a](../qwen35_pilot/README.md)). Only the scripts, configs,
logs and the first eval results stay here as a record.

## What was built

- **LoRA without the routed experts** (memory): attention q/k/v/o, linear-attention `in_proj_*` / `out_proj` and the
  shared-expert MLP; rank 16 on the last 16 of 40 layers (8.47M parameters). [`lora_moe.py`](lora_moe.py) launches
  `mlx_lm.lora` with the qwen35_pilot patches and adds a `stop_gradient` on the top-k expert indices (mlx-lm 0.31.3
  fails with "Cannot calculate VJP with respect to indices").
- **Export**: [`fix_experts.py`](fix_experts.py) maps MLX `switch_mlp.{gate,up,down}_proj` to the HF
  `experts.gate_up_proj` / `experts.down_proj` layout (streamed per shard, peak 8.8 GB), then `fix_norms.py` →
  `convert_hf_to_gguf.py --no-mtp` → Q4_K_M (21.2 GB). Ollama 0.40.15 runs it at ~59 tokens/s, 100% GPU.
- **Data** ([`make_data.py`](make_data.py)): the v18a mix under the Qwen3.5 tokenizer, capped at 2,600 tokens
  (`data_mix2600`, drops 1.9%; 3,200 ran out of memory). These copies are git-ignored and rebuildable.
- **Memory tests** (`memtest_*.yaml/.log`, `memtest_layers.sh`): seq 2600 fits with 16-40 LoRA layers (peak
  26.5-26.7 GB); more layers cost speed (104 → 53 tokens/s), not memory.

## Results

| Eval set | base (untuned) | pilot (200 it) | v1 | v2 | v18a-q4 (`latest`) |
| --- | --- | --- | --- | --- | --- |
| Hold-out (62) | 1 | 53 | 57 | 60 | **61** |
| v4 hold-out (25) | 0 | 12 | 18 | 19 | **20** |
| Fixed test (150) | | | 129 | 137 | **145** |
| complex_v8 test (100) | | | 63 | 82 | **88** |
| Complex schema-shift (150) | | | 80 | 94 | **126** |
| Real-style pairs (74) | | | 0 | 0 | 0 |

- **v1** (2026-11-15 16:58 → 21:10): 4,800 micro-batches, lr 1e-4 → 1e-5 (`lora_v1.yaml`). During this run the
  resume-seed bug was found: each restarted segment replayed the same batch order. Now fixed in `train_resumable.py`
  (`seed + 1000 * segment`).
- **v2** (11-16 01:40 → 06:00): v1 continued for 4,800 micro-batches, lr 5e-5 → 5e-6 (`lora_v2.yaml`). Better than v1
  everywhere but below v18a-q4 on every set.
- **v3** (11-16 19:38, `lora_v3.yaml`): LoRA on all 40 layers from v2, lr 1e-4. It **diverged** (val 0.007 → 0.15 by
  iteration 400, train loss spike to 5.8 at ~1,150), and at iteration ~2,850 the Mac mini rebooted with a watchdog
  kernel panic (23:39; training peak ~26.5 GB of 32 GB). Not resumed.

Lessons: a 35B MoE fits in 32 GB only with routed experts frozen and seq ≤ 2600, at ~3 s per micro-batch and almost no
memory headroom; lr 1e-4 on top of a converged adapter diverges.

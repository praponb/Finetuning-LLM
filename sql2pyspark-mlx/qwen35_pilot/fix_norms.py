"""Undo mlx-lm's +1 shift of the Qwen3.5 RMSNorm weights in a fused (dequantized) model, in place.

mlx-lm's qwen3_5 sanitize() adds 1 to input_layernorm / post_attention_layernorm / model.norm / q_norm / k_norm when
it converts the HF checkpoint (mlx-community's 4-bit weights already carry the shift), and mlx_lm.fuse saves them
that way. llama.cpp's converter expects the HF convention and adds 1 itself, so without this step the GGUF would get
the shift twice. linear_attn.norm (RMSNormGated) is not shifted by either side. The conv1d weights need no change:
llama.cpp squeezes them, which gives the same matrix for the HF [C, 1, K] and the mlx [C, K, 1] layout.

Usage:  python fix_norms.py <fused model dir>
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import mlx.core as mx

NORM_SUFFIXES = (".input_layernorm.weight", ".post_attention_layernorm.weight", "model.norm.weight",
                 ".q_norm.weight", ".k_norm.weight")

d = Path(sys.argv[1])
n = 0
for f in sorted(d.glob("*.safetensors")):
    w, meta = mx.load(str(f), return_metadata=True)
    hit = [k for k in w if k.endswith(NORM_SUFFIXES) and w[k].ndim == 1]
    for k in hit:
        if k.endswith("model.norm.weight"):     # sanity check: ~1 before (mlx), ~0 after (HF convention)
            print(f"{k}: mean {w[k].astype(mx.float32).mean().item():.3f} -> {(w[k] - 1.0).astype(mx.float32).mean().item():.3f}")
        w[k] = w[k] - 1.0
    if hit:   # mx.load is lazy (memory-mapped): write a new file, then replace the shard
        tmp = f.with_name(f.stem + ".tmp.safetensors")
        mx.save_safetensors(str(tmp), w, metadata=meta)
        del w
        os.replace(tmp, f)
    n += len(hit)
    print(f"{f.name}: {len(hit)} norm tensors shifted back")
print(f"total {n} (expected 32 x 2 + 8 x 2 + 1 = 81)")

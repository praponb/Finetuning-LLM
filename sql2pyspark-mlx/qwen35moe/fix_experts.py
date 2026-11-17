"""Undo mlx-lm's expert split in a dequantized / fused Qwen3.5-MoE model so llama.cpp can convert it, in place.

mlx-lm's qwen3_5_moe sanitize() turns HF's stacked experts
    mlp.experts.gate_up_proj [n_expert, 2*n_ff, n_embd]   and   mlp.experts.down_proj [n_expert, n_embd, n_ff]
into mlp.switch_mlp.{gate_proj, up_proj, down_proj}.weight, and mlx_lm.fuse / convert save them that way.
llama.cpp's converter (conversion/qwen.py) only maps the HF names, so this joins gate + up back into gate_up_proj
(gate first, as sanitize splits it) and renames down_proj. All new shards are written to temp files first and only
then swapped in, because a layer's gate and up can sit in different (memory-mapped) shards.

Usage:  python fix_experts.py <model dir>
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import mlx.core as mx

d = Path(sys.argv[1])
index_path = d / "model.safetensors.index.json"
index = json.loads(index_path.read_text())
wmap: dict[str, str] = index["weight_map"]

by_file: dict[str, list[str]] = defaultdict(list)
for key, fname in wmap.items():
    by_file[fname].append(key)

# One output shard at a time, with fresh lazy loads, so finished tensors are freed before the next shard
# (keeping them all alive grew to ~65 GB and macOS killed the process).
new_map: dict[str, str] = {}
tmp_files: dict[str, Path] = {}
n_gu = n_dn = 0
for fname in sorted(by_file):
    w, meta = mx.load(str(d / fname), return_metadata=True)
    tensors = {}
    for key in by_file[fname]:
        if ".switch_mlp.up_proj.weight" in key:
            continue                          # consumed together with its gate_proj
        if ".switch_mlp.gate_proj.weight" in key:
            up_key = key.replace(".gate_proj.", ".up_proj.")
            up = w[up_key] if wmap[up_key] == fname else mx.load(str(d / wmap[up_key]))[up_key]
            new_key = key.replace(".switch_mlp.gate_proj.weight", ".experts.gate_up_proj")
            tensors[new_key] = mx.concatenate([w[key], up], axis=-2)
            n_gu += 1
        elif ".switch_mlp.down_proj.weight" in key:
            new_key = key.replace(".switch_mlp.down_proj.weight", ".experts.down_proj")
            tensors[new_key] = w[key]
            n_dn += 1
        else:
            new_key = key
            tensors[key] = w[key]
        new_map[new_key] = fname
    tmp = d / (Path(fname).stem + ".tmp.safetensors")
    mx.save_safetensors(str(tmp), tensors, metadata=meta)
    tmp_files[fname] = tmp
    print(f"{fname}: {len(tensors)} tensors", flush=True)
    del w, tensors
    mx.clear_cache()

assert n_gu == n_dn and n_gu > 0, f"unexpected expert counts gate_up={n_gu} down={n_dn}"
for fname, tmp in tmp_files.items():
    os.replace(tmp, d / fname)
index["weight_map"] = new_map
index_path.write_text(json.dumps(index, indent=2) + "\n")
print(f"joined {n_gu} gate_up_proj and renamed {n_dn} down_proj tensors")

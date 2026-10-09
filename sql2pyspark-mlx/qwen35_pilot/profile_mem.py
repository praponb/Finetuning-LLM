"""Peak Metal memory of one LoRA training step (value_and_grad) of Qwen3.5-9B-4bit, like the pilot's settings.

Usage (from sql2pyspark-mlx/):  .venv/bin/python qwen35_pilot/profile_mem.py <seq_len> <loss: default|chunked> <gd_patch: 0|1> [num_layers]
The prompt is the first 40% of the sequence (masked), the loss covers the rest. One configuration per process, since a
Metal OOM leaves the process unusable.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn

sys.path.insert(0, str(Path(__file__).resolve().parent))
T, mode, patch = int(sys.argv[1]), sys.argv[2], sys.argv[3] == "1"
layers = int(sys.argv[4]) if len(sys.argv) > 4 else 8
if patch:
    import gd_patch
    gd_patch.install()
from chunked_loss import chunked_loss  # noqa: E402
from mlx_lm import load  # noqa: E402
from mlx_lm.tuner.trainer import default_loss, grad_checkpoint  # noqa: E402
from mlx_lm.tuner.utils import linear_to_lora_layers  # noqa: E402

model, _ = load("mlx-community/Qwen3.5-9B-4bit")
model.freeze()
linear_to_lora_layers(model, layers, {"rank": 16, "dropout": 0.05, "scale": 20.0})
model.train()
grad_checkpoint(model.layers[0])
loss = chunked_loss if mode == "chunked" else default_loss
vg = nn.value_and_grad(model, loss)

batch = mx.random.randint(0, 200000, (1, T + 1))
lengths = mx.array([[int(T * 0.4), T]])
mx.eval(batch, lengths)
base = mx.get_active_memory() / 1e9
mx.reset_peak_memory()
t0 = time.time()
(lv, n), g = vg(model, batch, lengths)
mx.eval(lv, g)
print(f"T={T} loss={mode} gd_patch={int(patch)} layers={layers}: weights {base:.2f} GB, step peak "
      f"{mx.get_peak_memory() / 1e9:.2f} GB, loss {lv.item():.3f}, {time.time() - t0:.1f} s", flush=True)

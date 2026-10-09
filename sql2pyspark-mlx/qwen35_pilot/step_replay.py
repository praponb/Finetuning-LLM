"""Replay the first training steps of the pilot run outside mlx_lm.lora, timing each step (watchdog debugging).

Same model / LoRA / loss / batches (seed 35, data_mix2600) as lora_config.yaml; prints the sequence length and the
time of every micro-batch so a GPU-watchdog abort can be tied to a step.

Usage (from sql2pyspark-mlx/):  .venv/bin/python qwen35_pilot/step_replay.py <steps> <compile: 0|1> [seed]
"""
from __future__ import annotations

import json
import sys
import time
from functools import partial
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gd_chunked  # noqa: E402
from chunked_loss import chunked_loss  # noqa: E402

gd_chunked.install()
from mlx_lm import load  # noqa: E402
from mlx_lm.tuner.datasets import CacheDataset, ChatDataset  # noqa: E402
from mlx_lm.tuner.trainer import grad_checkpoint, iterate_batches  # noqa: E402
from mlx_lm.tuner.utils import linear_to_lora_layers  # noqa: E402

steps, use_compile = int(sys.argv[1]), sys.argv[2] == "1"
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 35
np.random.seed(seed)
mx.random.seed(seed)
mx.set_cache_limit(1 << 30)

model, tok = load("mlx-community/Qwen3.5-9B-4bit")
model.freeze()
linear_to_lora_layers(model, 8, {"rank": 16, "dropout": 0.05, "scale": 20.0})
model.train()
grad_checkpoint(model.layers[0])
rows = [json.loads(l) for l in (HERE / "data_mix2600/train.jsonl").open()]
ds = CacheDataset(ChatDataset(rows, tok, mask_prompt=True))
opt = optim.AdamW(learning_rate=1e-4)
vg = nn.value_and_grad(model, chunked_loss)
state = [model.state, opt.state, mx.random.state]


def step(batch, lengths):
    (lv, n), g = vg(model, batch, lengths)
    opt.update(model, g)
    return lv, n


if use_compile:
    step = partial(mx.compile, inputs=state, outputs=state)(step)

for i, (batch, lengths) in enumerate(iterate_batches(ds, 1, 2600, loop=True, seed=seed)):
    if i >= steps:
        break
    t0 = time.time()
    lv, n = step(batch, lengths)
    mx.eval(state, lv, n)
    print(f"step {i + 1}: T={batch.shape[1]} targets={n.item()} loss={lv.item():.3f} {time.time() - t0:.1f} s "
          f"peak {mx.get_peak_memory() / 1e9:.2f} GB", flush=True)

"""Qwen3.5-35B-A3B feasibility, step 1: load in mlx-lm, list the LoRA target modules, check the training render, re-run the
gd_chunked gradient self-test on this model's Gated DeltaNet head sizes, and convert 3 hold-out queries (thinking off).

Usage (from the repo root):  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/qwen35moe/step1_check.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import mlx.core as mx
from mlx_lm import generate, load

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "sql2pyspark-mlx/qwen35_pilot"))
import gd_chunked  # noqa: E402
from mlx_lm.models import gated_delta as gd  # noqa: E402

MODEL = "mlx-community/Qwen3.5-35B-A3B-4bit"
SYSTEM = ("You are an expert data engineer specializing in SQL and Apache Spark. Convert the provided SQL query into "
          "clean, idiomatic PySpark DataFrame code.")

t0 = time.time()
model, tok = load(MODEL)
print(f"loaded in {time.time() - t0:.0f} s; active memory {mx.get_active_memory() / 2**30:.2f} GB")

# (a) module names of one linear-attention block and one full-attention block -> LoRA keys
layers = model.layers if hasattr(model, "layers") else model.language_model.model.layers
for i in (0, 3):
    names = sorted({n for n, _ in layers[i].named_modules() if n})
    print(f"=== block {i} modules ===\n  " + "\n  ".join(names))
args = model.args if hasattr(model, "args") else None
tc = getattr(args, "text_config", None) or {}
print("text_config keys of interest:", {k: tc.get(k) for k in ("linear_num_key_heads", "linear_num_value_heads",
      "linear_key_head_dim", "linear_value_head_dim", "num_experts", "num_experts_per_tok")} if isinstance(tc, dict) else tc)

# (b) training render vs inference prompt
sample = json.loads((ROOT / "sql2pyspark-mlx/complex_v17/data_mix/train.jsonl").open().readline())
msgs = sample["messages"]
full = tok.apply_chat_template(msgs, tokenize=False)
prompt_train = tok.apply_chat_template(msgs[:-1], tokenize=False, add_generation_prompt=True)
prompt_nothink = tok.apply_chat_template(msgs[:-1], tokenize=False, add_generation_prompt=True, enable_thinking=False)
print("=== mask offset prompt (mlx_lm.lora) ===\n" + repr(prompt_train[-80:]))
print("=== non-thinking inference prompt ===\n" + repr(prompt_nothink[-80:]))
print("full render starts with the non-thinking prompt:", full.startswith(prompt_nothink))
print("loss region:", repr(full[len(prompt_train):][:60]), "...")

# (c) gd_chunked vs mlx-lm reference on this model's head sizes
if isinstance(tc, dict) and tc.get("linear_num_key_heads"):
    Hk, Hv = tc["linear_num_key_heads"], tc["linear_num_value_heads"]
    Dk, Dv = tc["linear_key_head_dim"], tc["linear_value_head_dim"]
    mx.random.seed(0)
    B, T = 1, 150
    q = (mx.fast.rms_norm(mx.random.normal((B, T, Hk, Dk)), None, 1e-6) * Dk ** -1.0).astype(mx.bfloat16)
    k = (mx.fast.rms_norm(mx.random.normal((B, T, Hk, Dk)), None, 1e-6) * Dk ** -0.5).astype(mx.bfloat16)
    v = mx.random.normal((B, T, Hv, Dv)).astype(mx.bfloat16)
    a = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    b = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    A_log, dt_bias = mx.random.normal((Hv,)) * 0.5, mx.random.normal((Hv,)) * 0.5
    w = mx.random.normal((B, T, Hv, Dv))

    def loss(fn):
        def f(q, k, v, a, b):
            y, st = fn(q, k, v, a, b, A_log, dt_bias, None, None, False)
            return (y.astype(mx.float32) * w).sum() + st.sum()
        return f

    rv, rg = mx.value_and_grad(loss(gd.gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    nv, ng = mx.value_and_grad(loss(gd_chunked.gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    print(f"gd self-test (Hk={Hk}, Hv={Hv}, Dk={Dk}, Dv={Dv}): loss ref {rv.item():.4f} chunked {nv.item():.4f}")
    for name, r, n in zip("qkvab", rg, ng):
        r, n = r.astype(mx.float32), n.astype(mx.float32)
        print(f"  grad {name}: max rel diff {(mx.abs(r - n).max() / (mx.abs(r).max() + 1e-12)).item():.2e}")

# (d) three hold-out conversions, thinking off
cases = [json.loads(l) for l in (ROOT / "eval/holdout.jsonl").open()][:60:25]
for c in cases:
    m = [{"role": "system", "content": SYSTEM},
         {"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{c['sql']}\n```"}]
    p = tok.apply_chat_template(m, tokenize=False, add_generation_prompt=True, enable_thinking=False)
    t0 = time.time()
    out = generate(model, tok, p, max_tokens=700, verbose=False)
    n = len(tok.encode(out))
    print(f"\n=== {c['id']} ({time.time() - t0:.1f} s, {n} tokens) ===\n--- reply ---\n{out}")
    print("think leak:", "<think>" in out or "</think>" in out)
print(f"peak memory {mx.get_peak_memory() / 2**30:.2f} GB")

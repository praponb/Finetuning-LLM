"""Pilot step 1: load Qwen3.5-9B-4bit in mlx-lm, show how a training sample renders, convert 3 hold-out queries.

Checks that (a) mlx-lm loads the model, (b) the chat template mlx_lm.lora uses for training (apply_chat_template with no
extra arguments) matches the non-thinking prompt used at inference, and (c) the untuned model writes PySpark without a
<think> leak when thinking is off.

Usage (from the repo root):  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/qwen35_pilot/step1_check.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from mlx_lm import generate, load

ROOT = Path(__file__).resolve().parents[2]
MODEL = "mlx-community/Qwen3.5-9B-4bit"
SYSTEM = ("You are an expert data engineer specializing in SQL and Apache Spark. Convert the provided SQL query into "
          "clean, idiomatic PySpark DataFrame code.")

model, tok = load(MODEL)

# (b) training render vs inference prompt
sample = json.loads((ROOT / "sql2pyspark-mlx/schema_v15/data_mix/train.jsonl").open().readline())
msgs = sample["messages"]
full = tok.apply_chat_template(msgs, tokenize=False)
prompt_train = tok.apply_chat_template(msgs[:-1], tokenize=False, add_generation_prompt=True)
prompt_nothink = tok.apply_chat_template(msgs[:-1], tokenize=False, add_generation_prompt=True, enable_thinking=False)
print("=== full training render ===\n" + full)
print("=== mask offset prompt (mlx_lm.lora) ===\n" + repr(prompt_train[-80:]))
print("=== non-thinking inference prompt ===\n" + repr(prompt_nothink[-80:]))
print("full render starts with the non-thinking prompt:", full.startswith(prompt_nothink))
print("loss region:", repr(full[len(prompt_train):][:60]), "...")

# (c) three hold-out conversions, thinking off
cases = [json.loads(l) for l in (ROOT / "eval/holdout.jsonl").open()][:60:25]
for c in cases:
    m = [{"role": "system", "content": SYSTEM},
         {"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{c['sql']}\n```"}]
    p = tok.apply_chat_template(m, tokenize=False, add_generation_prompt=True, enable_thinking=False)
    t0 = time.time()
    out = generate(model, tok, p, max_tokens=600, verbose=False)
    print(f"\n=== {c['id']} ({time.time() - t0:.1f} s) ===\n{c['sql']}\n--- reply ---\n{out}")
    print("think leak:", "<think>" in out or "</think>" in out)

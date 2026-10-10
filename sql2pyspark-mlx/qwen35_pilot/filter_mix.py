"""Copy the v15 mix without the samples longer than MAX_TOKENS under the Qwen3.5 tokenizer (pilot step 3 data).

At 3,200 tokens a training step trips the macOS GPU watchdog ("Impacting Interactivity") on the M2; 2,600 passes.
246 of 25,008 train samples (1.0%) are longer than 2,600 tokens.

Usage (from the repo root):  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/qwen35_pilot/filter_mix.py [2600]
"""
import json
import sys
from pathlib import Path

from transformers import AutoTokenizer

MAX_TOKENS = int(sys.argv[1]) if len(sys.argv) > 1 else 2600
HERE = Path(__file__).resolve().parent
SRC, OUT = HERE.parent / "schema_v15/data_mix", HERE / f"data_mix{MAX_TOKENS}"
tok = AutoTokenizer.from_pretrained("mlx-community/Qwen3.5-9B-4bit")
OUT.mkdir(exist_ok=True)
for name in ("train", "valid"):
    rows = [json.loads(l) for l in (SRC / f"{name}.jsonl").open()]
    keep = [r for r in rows if len(tok.apply_chat_template(r["messages"], tokenize=True, return_dict=False)) <= MAX_TOKENS]
    with (OUT / f"{name}.jsonl").open("w") as f:
        for r in keep:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"{name}: kept {len(keep)} of {len(rows)}")

"""Calibration text for llama-imatrix (step A of the 11-23 improvement plan): N random train pairs of data_q9e rendered
exactly like the Ollama template of Modelfile.q35 (system, user, assistant with an empty think block). Train data
only, no eval data.

  .venv/bin/python qwen35_pilot/variants/make_calib.py [N=300] > qwen35_pilot/variants/calib_q9e.txt   (from sql2pyspark-mlx)
"""
import json
import random
import sys
from pathlib import Path

P = Path(__file__).resolve().parents[1]
n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
rows = [x for x in (P / "data_q9e" / "train.jsonl").read_text().splitlines() if x.strip()]
for x in random.Random(2033).sample(rows, n):
    m = json.loads(x)["messages"]
    sys.stdout.write(f"<|im_start|>system\n{m[0]['content']}<|im_end|>\n<|im_start|>user\n{m[1]['content']}<|im_end|>\n"
                     f"<|im_start|>assistant\n<think>\n\n</think>\n\n{m[2]['content']}<|im_end|>\n")

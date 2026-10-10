"""Pilot step 6: CPU-only (num_gpu 0) generation speed and load time of Ollama models on the Mac.

The server (Ubuntu, CPU only) runs v15-q4 at ~4.0 tok/s; its speed for another model is predicted as
4.0 x (model tok/s / v15-q4 tok/s) measured here under the same CPU-only settings.

Usage (from the repo root):  python3 sql2pyspark-mlx/qwen35_pilot/speed_test.py sql2pyspark:v15-q4 sql2pyspark:q35-pilot-q4
"""
from __future__ import annotations

import json
import sys

import requests

URL = "http://localhost:11434/api"
SERVER_V15_TOKS = 4.0
PROMPTS = [
    "SELECT id, full_name, city FROM patients WHERE insurance_type = 'PRIVATE';",
    "SELECT d.name, COUNT(*) AS n, AVG(a.duration_min) AS avg_min FROM appointments a JOIN doctors d ON a.doctor_id = d.id "
    "WHERE a.status = 'done' GROUP BY d.name HAVING COUNT(*) > 3 ORDER BY n DESC;",
    "SELECT id, flight_id, price, RANK() OVER (PARTITION BY flight_id ORDER BY price DESC) AS price_rank FROM bookings;",
]


def run(model: str) -> dict:
    for m in requests.get(f"{URL}/ps", timeout=30).json().get("models", []):   # cold start for the load time
        requests.post(f"{URL}/generate", json={"model": m["name"], "keep_alive": 0}, timeout=120)
    toks, secs, loads, prompt_rates = 0, 0.0, [], []
    for i, sql in enumerate(PROMPTS):
        r = requests.post(f"{URL}/chat", json={
            "model": model, "stream": False, **({"think": False} if "q35" in model else {}),
            "messages": [{"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{sql}\n```"}],
            "options": {"temperature": 0, "seed": 0, "num_gpu": 0, "num_predict": 400},
        }, timeout=1800).json()
        loads.append(r["load_duration"] / 1e9)
        toks += r["eval_count"]
        secs += r["eval_duration"] / 1e9
        prompt_rates.append(r["prompt_eval_count"] / (r["prompt_eval_duration"] / 1e9))
    requests.post(f"{URL}/generate", json={"model": model, "keep_alive": 0}, timeout=120)
    return {"model": model, "tok_s": toks / secs, "tokens": toks, "cold_load_s": loads[0],
            "prompt_tok_s": sum(prompt_rates) / len(prompt_rates)}


res = [run(m) for m in sys.argv[1:]]
base = next((r for r in res if r["model"].endswith("v15-q4")), res[0])
for r in res:
    r["server_pred_tok_s"] = SERVER_V15_TOKS * r["tok_s"] / base["tok_s"]
    print(json.dumps({k: round(v, 2) if isinstance(v, float) else v for k, v in r.items()}))

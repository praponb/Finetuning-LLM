"""Step C (11-23 improvement plan): ask a model to convert every pool query and record which answers are wrong.

Same prompt and decoding as eval/evaluate.py (evaluate.ask: temperature 0, seed 0, think false via --think), every
answer checked by verify_pairs.Checker against the SQL. Then extra samples at temperature 0.7 (seeds 1..K) for
--temp-n queries (all greedy failures first, then random others), to get more wrong and right answers.
Resumable: appends to the output JSONL, skips (id, seed) pairs already there.

  .venv/bin/python qwen35_pilot/selfplay/collect.py --model sql2pyspark:q9e-q4 [--temp-n 1500 --temp-k 2]
-> qwen35_pilot/selfplay/raw_<tag>.jsonl  {id, seed, temperature, reply, ok, status, detail, secs}
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "eval"))
import evaluate as ev  # noqa: E402  (ask, extract_code; puts sql2pyspark-mlx on sys.path)
from verify_pairs import Checker  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--pool", default=str(HERE / "pool.jsonl"))
    ap.add_argument("--temp-n", type=int, default=600)
    ap.add_argument("--temp-k", type=int, default=2)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--limit", type=int, default=0, help="only the first N pool queries (smoke test)")
    ap.add_argument("--num-predict", type=int, default=2048)
    ap.add_argument("--out", default="", help="output JSONL (default selfplay/raw_<tag>.jsonl)")
    a = ap.parse_args()
    pool = [json.loads(x) for x in Path(a.pool).read_text().splitlines() if x.strip()]
    if a.limit:
        pool = pool[:a.limit]
    out = Path(a.out) if a.out else HERE / f"raw_{a.model.split(':')[-1]}.jsonl"
    done = set()
    if out.exists():
        for x in out.read_text().splitlines():
            if x.strip():
                d = json.loads(x)
                done.add((d["id"], d["seed"]))
    ck = Checker()
    f = out.open("a")

    def run(item: dict, seed: int, temp: float) -> bool:
        extra = {"temperature": temp, "seed": seed} if seed else None
        t0 = time.time()
        try:
            reply, _ = ev.ask(a.model, item["sql"], 600, a.num_predict, extra, think=False)
        except Exception as e:  # noqa: BLE001
            reply = f"__ERROR__ {e}"
        code = ev.extract_code(reply)
        r = ck.check(item["sql"], code, item.get("order_cols", ()))
        rec = {"id": item["id"], "seed": seed, "temperature": temp if seed else 0, "reply": reply, "ok": r.ok,
               "status": r.status, "detail": r.detail[:300], "secs": round(time.time() - t0, 1)}
        f.write(json.dumps(rec) + "\n")
        f.flush()
        return r.ok

    greedy_ok = {}
    for x in out.read_text().splitlines() if out.exists() else []:
        d = json.loads(x)
        if d["seed"] == 0:
            greedy_ok[d["id"]] = d["ok"]
    t0, n = time.time(), 0
    for item in pool:
        if (item["id"], 0) in done:
            continue
        greedy_ok[item["id"]] = run(item, 0, 0)
        n += 1
        if n % 100 == 0:
            fails = sum(not v for v in greedy_ok.values())
            print(f"[{time.strftime('%H:%M:%S')}] greedy {len(greedy_ok)}/{len(pool)}, fails {fails}, "
                  f"{(time.time() - t0) / n:.1f} s/query", flush=True)
    fails = [i for i, ok in greedy_ok.items() if not ok]
    rest = [x["id"] for x in pool if greedy_ok.get(x["id"])]
    random.Random(2035).shuffle(rest)
    temp_ids = set(fails + rest[:max(0, a.temp_n - len(fails))])
    print(f"[{time.strftime('%H:%M:%S')}] greedy done: {len(fails)} fails of {len(greedy_ok)}; "
          f"temperature samples for {len(temp_ids)} queries x {a.temp_k}", flush=True)
    for k in range(1, a.temp_k + 1):
        for item in pool:
            if item["id"] in temp_ids and (item["id"], k) not in done:
                run(item, k, a.temperature)
        print(f"[{time.strftime('%H:%M:%S')}] temperature seed {k} done", flush=True)
    print("COLLECT DONE", flush=True)


if __name__ == "__main__":
    main()

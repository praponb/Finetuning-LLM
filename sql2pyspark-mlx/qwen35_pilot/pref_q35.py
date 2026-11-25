"""Preference round (ORPO) for the Qwen3.5-9B LoRA adapters (11-23 improvement plan, step D).

mlx-lm 0.31.3 has no DPO / ORPO (and mlx-lm-lora needs MLX >= 0.32.3, which this project avoids), so this is an own
loop on the same pieces as lora_q35.py: gd_chunked (training-mode Gated DeltaNet), chunked_loss (chunked lm_head +
masked CE), mlx_lm.load / linear_to_lora_layers / grad_checkpoint, AdamW, mlx-lm's tokenization (ChatDataset:
apply_chat_template of the full chat, prompt masked up to the generation prompt).

Loss per micro-batch:
  preference pair:  NLL(chosen) + lam * -log sigmoid(logodds(chosen) - logodds(rejected))      (ORPO, Hong et al. 2024)
                    logodds(y) = logp - log(1 - exp(logp)), logp = mean token log-prob of the answer
  replay pair:      NLL(answer)                                                            (keeps the broad skills)
Batch 1, grad accumulation, cosine lr with warmup over the optimizer updates. The micro-batch order is a fixed
function of --seed, so a restart after a Metal abort resumes at the newest checkpoint with the same order (optimizer
state restarts, as with train_resumable.py). Saves adapters.safetensors + adapter_config.json (mlx-lm format) and
<iter>_adapters.safetensors checkpoints, so mlx_lm.fuse works unchanged.

  .venv/bin/python qwen35_pilot/pref_q35.py --resume-adapter-dir qwen35_pilot/adapters_q9e --out qwen35_pilot/adapters_q9f \
      --data qwen35_pilot/data_q9f --replay qwen35_pilot/data_q9e/train.jsonl [--limit-pairs 20 --max-iters 40]
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import sys
import time
from pathlib import Path

if not os.environ.get("Q35_SMALL_BUFFERS"):
    os.environ.pop("MLX_MAX_OPS_PER_BUFFER", None)
    os.environ.pop("MLX_MAX_MB_PER_BUFFER", None)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import gd_chunked  # noqa: E402

gd_chunked.install()
import mlx.core as mx  # noqa: E402
import mlx.nn as nn  # noqa: E402
import mlx.optimizers as optim  # noqa: E402
from chunked_loss import chunked_loss  # noqa: E402
from mlx.utils import tree_flatten, tree_map  # noqa: E402
from mlx_lm import load  # noqa: E402
from mlx_lm.tuner.trainer import grad_checkpoint  # noqa: E402
from mlx_lm.tuner.utils import linear_to_lora_layers  # noqa: E402

mx.set_cache_limit(512 << 20)
mx.set_memory_limit(int(os.environ.get("Q35_MEM_LIMIT_GB", "12")) << 30)


def encode(tok, messages, answer, max_tokens):
    full = tok.apply_chat_template(messages + [{"role": "assistant", "content": answer}], return_dict=False)
    off = len(tok.apply_chat_template(messages, add_generation_prompt=True, return_dict=False))
    if len(full) > max_tokens or off >= len(full):
        return None
    pad = min(1 + 32 * ((len(full) + 31) // 32), max_tokens)          # as mlx-lm's iterate_batches
    arr = full + [0] * (pad - len(full))
    return mx.array([arr]), mx.array([[off, len(full)]])


def logodds(nll):                       # nll = -mean log p  ->  log(p / (1 - p)), p = exp(-nll)
    logp = -nll
    return logp - mx.log1p(-mx.exp(mx.minimum(logp, -1e-6)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="mlx-community/Qwen3.5-9B-4bit")
    ap.add_argument("--resume-adapter-dir", required=True, help="adapter dir to continue (its config sets the shape)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--data", required=True, help="dir with pref_train.jsonl / pref_valid.jsonl (make_pref.py)")
    ap.add_argument("--replay", required=True, help="SFT jsonl ({messages}) mixed in as plain NLL micro-batches")
    ap.add_argument("--replay-ratio", type=float, default=3.0)
    ap.add_argument("--epochs", type=int, default=2)
    ap.add_argument("--max-iters", type=int, default=2500)
    ap.add_argument("--lam", type=float, default=0.1)
    ap.add_argument("--lr", type=float, default=2e-6)
    ap.add_argument("--lr-end", type=float, default=2e-7)
    ap.add_argument("--warmup", type=int, default=20, help="optimizer updates")
    ap.add_argument("--grad-accum", type=int, default=4)
    ap.add_argument("--max-tokens", type=int, default=3200)
    ap.add_argument("--save-every", type=int, default=100)
    ap.add_argument("--eval-every", type=int, default=500)
    ap.add_argument("--report-every", type=int, default=25)
    ap.add_argument("--seed", type=int, default=49)
    ap.add_argument("--limit-pairs", type=int, default=0, help="sanity run: only the first N train pairs")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    cfg = json.loads((Path(a.resume_adapter_dir) / "adapter_config.json").read_text())
    rd = lambda p: [json.loads(x) for x in Path(p).read_text().splitlines() if x.strip()]
    pref_tr, pref_va = rd(Path(a.data) / "pref_train.jsonl"), rd(Path(a.data) / "pref_valid.jsonl")
    if a.limit_pairs:
        pref_tr = pref_tr[:a.limit_pairs]
    rng = random.Random(a.seed)
    replay_all = rd(a.replay)
    n_replay = int(len(pref_tr) * a.replay_ratio)
    n_replay = max(0, min(n_replay, a.max_iters - a.epochs * len(pref_tr)))
    items = [("pref", i) for _ in range(a.epochs) for i in range(len(pref_tr))]
    items += [("sft", i) for i in rng.sample(range(len(replay_all)), n_replay)]
    rng.shuffle(items)
    items = items[:a.max_iters]
    total, n_updates = len(items), math.ceil(len(items) / a.grad_accum)

    # resume point: newest checkpoint of this run
    cks = sorted((int(m.group(1)), p) for p in out.glob("*_adapters.safetensors")
                 if (m := re.match(r"(\d+)_adapters", p.name)))
    start = cks[-1][0] if cks else 0

    model, tok = load(a.base, tokenizer_config={"trust_remote_code": True})
    model.freeze()
    linear_to_lora_layers(model, cfg["num_layers"], cfg["lora_parameters"])
    src = cks[-1][1] if cks else Path(a.resume_adapter_dir) / "adapters.safetensors"
    model.load_weights(str(src), strict=False)
    print(f"[pref] {total} micro-batches ({sum(k == 'pref' for k, _ in items)} preference, "
          f"{sum(k == 'sft' for k, _ in items)} replay), {n_updates} updates; start at {start} from {src}", flush=True)
    new_cfg = dict(cfg) | {"adapter_path": str(out), "resume_adapter_file": str(src), "pref": vars(a)}
    (out / "adapter_config.json").write_text(json.dumps(new_cfg, indent=4))
    grad_checkpoint(model.layers[0])

    def sched(u):
        if u < a.warmup:
            return a.lr * (u + 1) / a.warmup
        t = min(1.0, (u - a.warmup) / max(1, n_updates - a.warmup))
        return a.lr_end + 0.5 * (a.lr - a.lr_end) * (1 + math.cos(math.pi * t))

    opt = optim.AdamW(learning_rate=sched(start // a.grad_accum))

    def pref_loss(model, bc, lc, br, lr_):
        nll_c, _ = chunked_loss(model, bc, lc)
        nll_r, _ = chunked_loss(model, br, lr_)
        margin = logodds(nll_c) - logodds(nll_r)
        loss = nll_c - a.lam * nn.log_sigmoid(margin)
        return loss, (nll_c, nll_r, margin)

    def sft_loss(model, b, l):
        nll, _ = chunked_loss(model, b, l)
        return nll, (nll,)

    vg_pref = nn.value_and_grad(model, lambda m, *x: pref_loss(m, *x)[0])
    vg_sft = nn.value_and_grad(model, lambda m, *x: sft_loss(m, *x)[0])

    def evaluate():
        model.eval()
        acc = n = 0
        s_nll = s_m = 0.0
        for p in pref_va:
            c = encode(tok, p["messages"], p["chosen"], a.max_tokens)
            r = encode(tok, p["messages"], p["rejected"], a.max_tokens)
            if not c or not r:
                continue
            _, (nc, nr, m) = pref_loss(model, *c, *r)
            mx.eval(nc, nr, m)
            n += 1
            s_nll += nc.item()
            s_m += m.item()
            acc += nc.item() < nr.item()
        model.train()
        return {"pairs": n, "nll_chosen": round(s_nll / max(n, 1), 4), "margin": round(s_m / max(n, 1), 4),
                "acc": round(acc / max(n, 1), 4)}

    if start == 0:
        print(f"[pref] val at 0: {evaluate()}", flush=True)
    model.train()
    grad, t0, stats, skipped = None, time.time(), [], 0
    for it in range(start, total):
        kind, i = items[it]
        if kind == "pref":
            p = pref_tr[i]
            c = encode(tok, p["messages"], p["chosen"], a.max_tokens)
            r = encode(tok, p["messages"], p["rejected"], a.max_tokens)
            if not c or not r:
                skipped += 1
                continue
            loss, g = vg_pref(model, *c, *r)
        else:
            x = replay_all[i]["messages"]
            e = encode(tok, x[:-1], x[-1]["content"], a.max_tokens)
            if not e:
                skipped += 1
                continue
            loss, g = vg_sft(model, *e)
        grad = g if grad is None else tree_map(lambda u, v: u + v, grad, g)
        if (it + 1) % a.grad_accum == 0 or it + 1 == total:
            opt.learning_rate = sched(it // a.grad_accum)
            opt.update(model, tree_map(lambda u: u / a.grad_accum, grad))
            grad = None
        mx.eval(model.parameters(), opt.state, loss, *([grad] if grad is not None else []))   # no graph build-up
        stats.append((kind, loss.item()))
        if (it + 1) % a.report_every == 0:
            pl = [v for k, v in stats if k == "pref"]
            sl = [v for k, v in stats if k == "sft"]
            print(f"[pref] iter {it + 1}/{total}: pref loss {sum(pl) / max(len(pl), 1):.4f} ({len(pl)}), "
                  f"replay loss {sum(sl) / max(len(sl), 1):.4f} ({len(sl)}), lr {opt.learning_rate.item():.2e}, "
                  f"{(time.time() - t0) / len(stats):.1f} s/it, peak {mx.get_peak_memory() / 2**30:.2f} GB, "
                  f"skipped {skipped}", flush=True)
            stats = []
            t0 = time.time()
        if (it + 1) % a.save_every == 0 or it + 1 == total:
            w = dict(tree_flatten(model.trainable_parameters()))
            mx.save_safetensors(str(out / f"{it + 1:07d}_adapters.safetensors"), w)
            mx.save_safetensors(str(out / "adapters.safetensors"), w)
        if (it + 1) % a.eval_every == 0 and it + 1 != total:
            print(f"[pref] val at {it + 1}: {evaluate()}", flush=True)
    print(f"[pref] val at end: {evaluate()}", flush=True)
    print("[pref] finished", flush=True)


if __name__ == "__main__":
    main()

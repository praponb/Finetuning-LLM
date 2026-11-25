"""Average fused (dequantized, fix_norms-applied) model folders tensor by tensor (11-23 improvement plan, step B).

All inputs are fusions of LoRA adapters onto the SAME dequantized 4-bit base, so the plain average of the weights is
base + the average LoRA change (exact; averaging the LoRA A / B matrices would not be). Tensors are averaged in float32
and cast back to their dtype, one shard at a time. Non-weight files (config, tokenizer, ...) come from the first input.

  .venv/bin/python qwen35_pilot/variants/avg_weights.py OUT IN1 IN2 [...] [--weights 0.5 0.5]
  .venv/bin/python qwen35_pilot/variants/avg_weights.py --selftest IN     (IN averaged with itself must be identical)
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import mlx.core as mx


def average(out: Path, ins: list[Path], weights: list[float]) -> None:
    assert abs(sum(weights) - 1) < 1e-6 and len(weights) == len(ins)
    shards = sorted(p.name for p in ins[0].glob("*.safetensors"))
    for d in ins[1:]:
        assert sorted(p.name for p in d.glob("*.safetensors")) == shards, f"shard names differ: {d}"
    out.mkdir(parents=True, exist_ok=True)
    for f in ins[0].iterdir():
        if f.is_file() and f.suffix != ".safetensors" and f.name != ".fused_ok":
            shutil.copy(f, out / f.name)
    for s in shards:
        tensors = [mx.load(str(d / s)) for d in ins]
        keys = set(tensors[0])
        assert all(set(t) == keys for t in tensors), f"tensor names differ in {s}"
        res = {}
        for k in keys:
            acc = sum(w * t[k].astype(mx.float32) for w, t in zip(weights, tensors))
            res[k] = acc.astype(tensors[0][k].dtype)
        mx.eval(res)
        mx.save_safetensors(str(out / s), res, metadata={"format": "mlx"})
        del tensors, res
        print(f"  {s}: {len(keys)} tensors", flush=True)


def selftest(d: Path) -> None:
    tmp = d.parent / (d.name + "_selftest")
    average(tmp, [d, d], [0.5, 0.5])
    bad = 0
    for s in sorted(p.name for p in d.glob("*.safetensors")):
        a, b = mx.load(str(d / s)), mx.load(str(tmp / s))
        bad += sum(1 for k in a if not mx.array_equal(a[k], b[k]).item())
    shutil.rmtree(tmp)
    print(f"selftest {d.name}: {'OK, identical' if bad == 0 else f'{bad} tensors differ'}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--weights", type=float, nargs="*")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest(Path(a.paths[0]))
    out, ins = Path(a.paths[0]), [Path(p) for p in a.paths[1:]]
    average(out, ins, a.weights or [1 / len(ins)] * len(ins))
    print(f"wrote {out}")

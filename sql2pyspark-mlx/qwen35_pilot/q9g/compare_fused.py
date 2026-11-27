"""Max / mean abs difference between two fused model folders, tensor by tensor (q9g step 0, check A)."""
import sys
from pathlib import Path
import mlx.core as mx
a, b = Path(sys.argv[1]), Path(sys.argv[2])
worst, tot, n, cnt = 0.0, 0.0, 0, 0
for f in sorted(a.glob("*.safetensors")):
    x, y = mx.load(str(f)), mx.load(str(b / f.name))
    assert sorted(x) == sorted(y), f.name
    for k in x:
        d = mx.abs(x[k].astype(mx.float32) - y[k].astype(mx.float32))
        m = d.max().item(); worst = max(worst, m); tot += d.sum().item(); n += d.size
        if m > 0: cnt += 1
        if m > 1e-2: print("BIG", k, m)
print(f"max_abs {worst:.3e}  mean_abs {tot / n:.3e}  tensors_with_diff {cnt}")

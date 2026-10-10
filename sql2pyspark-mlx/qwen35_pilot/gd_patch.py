"""Memory-bounded training for Qwen3.5's Gated DeltaNet layers in mlx-lm 0.31.3 (pilot only).

In training mode mlx-lm runs the gated delta rule as a Python loop over the tokens (gated_delta_ops), and autograd
keeps several [B, 32, 128, 128] fp32 states per token for the backward pass: ~8 MB per token per layer, so even
max_seq_length 2600 ran out of Metal memory on the 16 GB M2 (pilot step 2).

install() replaces qwen3_5.gated_delta_update with a version whose
- forward is the fused Metal kernel mlx-lm uses for inference (fast, no per-token graph), and
- backward (custom VJP) recomputes the recurrence with the reference ops in chunks of CHUNK tokens, last chunk first,
  so only the chunk-boundary states (~2 MB each) and one chunk's graph at a time are kept.
The kernel and the ops compute the same recurrence; check() compares outputs and gradients.
"""
from __future__ import annotations

import mlx.core as mx
from mlx_lm.models import gated_delta as gd
from mlx_lm.models import qwen3_5

CHUNK = 64


FWD_CHUNK = 512   # forward kernel dispatches of at most 512 tokens (long single dispatches trip the macOS GPU watchdog)


@mx.custom_function
def kernel_fwd(q, k, v, g, beta, state):
    ys = []
    for s in range(0, q.shape[1], FWD_CHUNK):
        e = s + FWD_CHUNK
        y, state = gd.gated_delta_kernel(q[:, s:e], k[:, s:e], v[:, s:e], g[:, s:e], beta[:, s:e], state, None)
        ys.append(y)
    return mx.concatenate(ys, axis=1), state


@kernel_fwd.vjp
def _kernel_vjp(primals, cotangents, outputs):
    """Reverse pass over chunks of CHUNK tokens: the chunk-boundary states come from the kernel; each chunk is
    recomputed with the reference ops and differentiated with mx.vjp. mx.depends makes chunk i's recompute wait for
    all gradients of chunk i + 1, so the lazy evaluator holds one chunk's graph at a time (with plain mx.checkpoint
    per chunk, or without the dependency on dq / dk / dv / dg / db, the saved states of every chunk stayed alive
    until the final concatenation: 1.5-4 MB per token)."""
    q, k, v, g, beta, state = primals
    dy, dstate = cotangents
    T = q.shape[1]
    bounds = list(range(0, T, CHUNK))
    states = [state]
    for s in bounds[:-1]:
        e = s + CHUNK
        states.append(gd.gated_delta_kernel(q[:, s:e], k[:, s:e], v[:, s:e], g[:, s:e], beta[:, s:e], states[-1])[1])
    grads = {n: [] for n in "qkvgb"}
    for s, st in zip(reversed(bounds), reversed(states)):
        e = min(s + CHUNK, T)
        st = mx.depends(st, dstate)
        _, (dq, dk, dv, dg, db, dstate) = mx.vjp(
            lambda q_, k_, v_, g_, b_, st_: list(gd.gated_delta_ops(q_, k_, v_, g_, b_, st_, None)),
            [q[:, s:e], k[:, s:e], v[:, s:e], g[:, s:e], beta[:, s:e], st],
            [dy[:, s:e], dstate])
        dstate = mx.depends(dstate, [dq, dk, dv, dg, db])   # finish this chunk's grads before the next recompute
        for n, d in zip("qkvgb", (dq, dk, dv, dg, db)):
            grads[n].append(d)
    return tuple(mx.concatenate(grads[n][::-1], axis=1) for n in "qkvgb") + (dstate,)


def gated_delta_update(q, k, v, a, b, A_log, dt_bias, state=None, mask=None, use_kernel=True):
    if use_kernel or mask is not None or not mx.metal.is_available():   # inference / padded batches: unchanged
        return gd.gated_delta_update(q, k, v, a, b, A_log, dt_bias, state, mask, use_kernel)
    beta = mx.sigmoid(b)
    g = gd.compute_g(A_log, a, dt_bias)
    if state is None:
        B, _, _, Dk = q.shape
        Hv, Dv = v.shape[-2:]
        state = mx.zeros((B, Hv, Dv, Dk), dtype=mx.float32)
    y, state = kernel_fwd(q, k, v, g, beta, state)
    return y, state


def install():
    qwen3_5.gated_delta_update = gated_delta_update


def check(T=150, seed=0):
    """Outputs and input gradients of the patched path vs the plain reference ops (Qwen3.5-9B head sizes)."""
    mx.random.seed(seed)
    B, Hk, Hv, D = 1, 16, 32, 128
    q = (mx.random.normal((B, T, Hk, D)) * 0.1).astype(mx.bfloat16)
    k = (mx.random.normal((B, T, Hk, D)) * 0.1).astype(mx.bfloat16)
    v = mx.random.normal((B, T, Hv, D)).astype(mx.bfloat16)
    a = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    b = mx.random.normal((B, T, Hv)).astype(mx.bfloat16)
    A_log = mx.random.normal((Hv,)) * 0.5
    dt_bias = mx.random.normal((Hv,)) * 0.5
    w = mx.random.normal((B, T, Hv, D))

    def loss(fn):
        def f(q, k, v, a, b):
            y, _ = fn(q, k, v, a, b, A_log, dt_bias, None, None, False)
            return (y.astype(mx.float32) * w).sum()
        return f

    ref_val, ref_g = mx.value_and_grad(loss(gd.gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    new_val, new_g = mx.value_and_grad(loss(gated_delta_update), argnums=(0, 1, 2, 3, 4))(q, k, v, a, b)
    print(f"loss ref {ref_val.item():.4f}  patched {new_val.item():.4f}")
    for name, r, n in zip("qkvab", ref_g, new_g):
        r, n = r.astype(mx.float32), n.astype(mx.float32)
        rel = (mx.abs(r - n).max() / (mx.abs(r).max() + 1e-12)).item()
        print(f"grad {name}: max rel diff {rel:.2e}")


if __name__ == "__main__":
    check()

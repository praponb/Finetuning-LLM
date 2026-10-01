"""Rewrite PySpark code from the hoisted window style to the "mirror" style (classic v1-v7 pairs -> v10).

Hoisted (v1-v8 classic data):            Mirror (complex_v9 data, now also classic):
    w = Window.partitionBy("a")              ... F.sum("x").over(Window.partitionBy("a")) ...
    w2 = w.rowsBetween(-2, 0)                ... F.avg("x").over(Window.partitionBy("a").rowsBetween(-2, 0)) ...
    ... .over(w) ... .over(w2) ...

A window variable stays a variable only when the SQL names that window (`WINDOW w AS (...)`); every other one is
substituted where it is used and its definition line is dropped. Variables defined from other window variables
(`w2 = w.rowsBetween(...)`) are resolved first. The SQL is never changed.
"""
from __future__ import annotations

import ast
import io
import re
import tokenize

DEF = re.compile(r"^([A-Za-z_]\w*) = (.+)$")


def _named_in_sql(sql: str, name: str) -> bool:
    return bool(re.search(r"\bWINDOW\b", sql, re.I)) and bool(re.search(rf"\b{re.escape(name)}\s+AS\s*\(", sql, re.I))


def _substitute(code: str, defs: dict) -> str:
    """Replace bare NAME tokens (not attributes, not keyword arguments) that are inlined window variables."""
    toks = list(tokenize.generate_tokens(io.StringIO(code).readline))
    edits = []
    for i, t in enumerate(toks):
        if t.type != tokenize.NAME or t.string not in defs:
            continue
        prev = toks[i - 1].string if i else ""
        nxt = toks[i + 1].string if i + 1 < len(toks) else ""
        if prev == "." or nxt == "=":
            continue
        edits.append((t.start, t.end, defs[t.string]))
    lines = code.split("\n")
    for (r0, c0), (r1, c1), rep in sorted(edits, reverse=True):
        assert r0 == r1
        ln = lines[r0 - 1]
        lines[r0 - 1] = ln[:c0] + rep + ln[c1:]
    return "\n".join(lines)


def mirror_code(sql: str, code: str) -> str | None:
    """The mirror-style version of `code`, the same code if it has no window variables, or None if it can't be
    rewritten safely (multi-line definitions)."""
    if "Window" not in code:
        return code
    if re.search(r"^\w+ = \(\s*$\n\s*Window", code, re.M):
        return None
    defs: dict = {}       # inlined name -> expression
    kept, out = set(), []
    for line in code.split("\n"):
        m = DEF.match(line)
        if m:
            name, expr = m.groups()
            head = expr.split(".")[0].split("(")[0]
            if expr.startswith("Window") or (head in defs or head in kept) and expr.startswith(head + "."):
                expr = _substitute(expr, defs) if defs else expr
                if _named_in_sql(sql, name):
                    kept.add(name)
                    out.append(f"{name} = {expr}")
                else:
                    defs[name] = expr
                continue
        out.append(line)
    if not defs:
        return code
    new = _substitute("\n".join(out), defs)
    new = re.sub(r"\n{3,}", "\n\n", new).replace("import Window\n\n\n", "import Window\n\n")
    ast.parse(new)
    return new


if __name__ == "__main__":
    demo_sql = "SELECT a, SUM(x) OVER (PARTITION BY a) AS s FROM t"
    demo = ('from pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n\n'
            'w = Window.partitionBy("a")\nw2 = w.rowsBetween(-2, 0)\n\n'
            'result = df_t.select("a", F.sum("x").over(w).alias("s"), F.avg("x").over(w2).alias("m"))\n')
    print(mirror_code(demo_sql, demo))

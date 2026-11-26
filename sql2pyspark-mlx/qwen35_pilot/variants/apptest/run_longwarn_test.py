"""AppTest for the long-input warning + cut-off notice (app.py in argv[1]). Kimi off (no .env here).
A: short SQL, real Ollama call -> code, no warnings.  B: 14k-char SQL -> warning before converting (no call).
C: mocked reply with done_reason=length -> cut-off notice, no auto repair.  D: mocked done_reason=stop -> no notice."""
import sys
from unittest import mock
from streamlit.testing.v1 import AppTest

APP = sys.argv[1] + "/app.py"
LONG_SQL = "WITH " + ",\n".join(f"c{i} AS (SELECT id, amount_{i} FROM t{i} WHERE flag_{i} = 1)" for i in range(230)) \
    + "\nSELECT * FROM c0"
results = []

def fresh(sql):
    at = AppTest.from_file(APP, default_timeout=300)
    at.run()
    at.sidebar.text_input[0].set_value("sql2pyspark:latest")
    at.text_area[0].set_value(sql).run()
    return at

def has(at, word):
    return any(word in w.value for w in at.warning)

def check(name, cond):
    results.append(cond); print(("PASS " if cond else "FAIL ") + name)

# A
at = fresh("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE city = 'Bangkok');")
check("A short: no long warning before convert", not has(at, "very long"))
at.button[0].click().run()
check("A short: code returned, no cut-off notice, no error",
      bool(at.code) and not has(at, "reply was cut off") and not at.error)

# B
at = fresh(LONG_SQL)
print(f"   long SQL = {len(LONG_SQL):,} chars")
check("B long: warning shown before convert", has(at, "very long"))
check("B long: no cut-off notice yet", not has(at, "reply was cut off"))

def fake(done_reason):
    r = mock.Mock(status_code=200)
    r.json.return_value = {"message": {"content": "```python\nresult = df_t0.select('id')\n```"}, "done_reason": done_reason}
    return r

# C
at = fresh(LONG_SQL)
with mock.patch("requests.post", return_value=fake("length")):
    at.button[0].click().run()
check("C length: cut-off notice shown", has(at, "reply was cut off"))
check("C length: converted.truncated = True, no auto repair queued",
      at.session_state["converted"]["truncated"] is True and not at.session_state["auto_repair"]
      if "auto_repair" in at.session_state else at.session_state["converted"]["truncated"] is True)

# D
at = fresh("SELECT a FROM t")
with mock.patch("requests.post", return_value=fake("stop")):
    at.button[0].click().run()
check("D stop: no cut-off notice", not has(at, "reply was cut off") and bool(at.code))

print("LONGWARN TEST", "PASS" if all(results) else f"FAIL ({results.count(False)})")

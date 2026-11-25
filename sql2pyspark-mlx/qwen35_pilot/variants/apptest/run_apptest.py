"""Drive the patched app (think: False) through Streamlit's AppTest with " + (sys.argv[2] if len(sys.argv) > 2 else "sql2pyspark:q9df3-q4") + ". Kimi off (no .env here)."""
import sys, time
from streamlit.testing.v1 import AppTest
SQLS = [
    "SELECT city, COUNT(*) AS n FROM customers GROUP BY city HAVING COUNT(*) > 1;",
    "SELECT id, passenger_name, CONCAT(seat_class, '-', COALESCE(loyalty_tier, 'NONE')) AS fare_code FROM bookings;",
    "SELECT p.full_name, COUNT(a.id) AS visits FROM patients p LEFT JOIN appointments a ON a.patient_id = p.id "
    "GROUP BY p.full_name ORDER BY visits DESC LIMIT 5;",
    "SELECT * FROM orders o WHERE o.id NOT IN (SELECT order_id FROM refunds WHERE order_id IS NOT NULL);",
    "SELECT * FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);",
    "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100);",
    "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE city = 'Bangkok');",
]
bad = 0
for sql in SQLS:
    at = AppTest.from_file(sys.argv[1] + "/app.py", default_timeout=600)
    at.run()
    at.sidebar.text_input[0].set_value("" + (sys.argv[2] if len(sys.argv) > 2 else "sql2pyspark:q9df3-q4") + "")
    at.text_area[0].set_value(sql)
    t = time.time()
    at.button[0].click().run()
    codes = [c.value for c in at.code]
    errs = [e.value for e in at.error]
    warns = [w.value for w in at.warning]
    caps = [c.value for c in at.caption if "Basic checks" in c.value]
    ok = bool(codes) and not errs and "<think>" not in "".join(codes)
    bad += not ok
    print(f"{time.time()-t:5.1f} s  {'OK ' if ok else 'BAD'}  checks={'passed' if caps else warns[:1]}  errors={errs[:1]}  "
          f"code_lines={len(codes[0].splitlines()) if codes else 0}  | {sql[:60]}")
print("APPTEST", "PASS" if not bad else f"FAIL ({bad})")

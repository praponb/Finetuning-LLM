SELECT id, carrier FROM shipments WHERE order_id = 27 UNION SELECT id, carrier FROM shipments WHERE shipped_date > '2025-01-29' ORDER BY carrier DESC;

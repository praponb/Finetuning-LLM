SELECT shipped_date, delivered_date, cost FROM shipments WHERE order_id = 30 AND order_id IN (1, 14, 16) AND delivered_date < '2024-09-01'

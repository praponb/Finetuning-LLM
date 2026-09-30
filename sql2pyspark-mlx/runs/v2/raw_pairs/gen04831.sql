SELECT id, carrier, delivered_date, cost, LEAD(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS next_cost FROM shipments WHERE order_id IN (4, 11, 33) ORDER BY id DESC

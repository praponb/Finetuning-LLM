SELECT id, order_id, carrier, shipped_date, cost, LAG(cost, 2, 0) OVER (PARTITION BY order_id, carrier ORDER BY shipped_date, id) AS prev_cost FROM shipments

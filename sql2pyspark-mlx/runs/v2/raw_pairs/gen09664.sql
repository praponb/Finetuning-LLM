SELECT status, carrier, MAX(cost) AS max_cost FROM shipments WHERE order_id IS NOT NULL GROUP BY status, carrier HAVING max_cost > 85;

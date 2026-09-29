SELECT order_id, MAX(cost) AS max_cost, MIN(cost) AS min_cost FROM shipments WHERE order_id IS NULL AND cost < 36 GROUP BY order_id ORDER BY 3 DESC;

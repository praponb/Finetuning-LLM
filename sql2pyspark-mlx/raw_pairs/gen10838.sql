SELECT order_id, COUNT(cost) AS cost_count, MAX(cost) AS max_cost FROM shipments GROUP BY order_id;

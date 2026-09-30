SELECT order_id, MIN(cost) AS min_cost FROM shipments GROUP BY order_id;

SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost FROM shipments GROUP BY 1

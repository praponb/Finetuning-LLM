SELECT order_id, SUM(cost) total_cost, SUM(SUM(cost)) OVER (ORDER BY order_id) AS cumulative_cost
FROM shipments
GROUP BY order_id;

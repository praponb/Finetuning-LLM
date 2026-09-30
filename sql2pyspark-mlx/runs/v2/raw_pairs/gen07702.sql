SELECT order_id, cost_range, avg_cost
FROM (
    SELECT order_id, MAX(cost) - MIN(cost) AS cost_range, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
) AS sub
WHERE cost_range < 36
ORDER BY cost_range, order_id
LIMIT 3;

SELECT *
FROM (
    SELECT order_id, MAX(cost) AS max_cost
    FROM shipments
    GROUP BY order_id
) AS s
WHERE max_cost < 89
ORDER BY order_id
LIMIT 5

SELECT *
FROM (
    SELECT order_id, ROUND(STDDEV(cost), 2) AS stddev_cost, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
) AS sub
WHERE stddev_cost >= 45
ORDER BY stddev_cost DESC

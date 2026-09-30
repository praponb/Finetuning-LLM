WITH OrderStats AS (
    SELECT order_id, MAX(cost) - MIN(cost) AS cost_range, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    O.cost_range,
    O.stddev_cost
FROM orders o
LEFT JOIN OrderStats O ON o.id = O.order_id
WHERE O.cost_range >= 80
ORDER BY cost_range DESC;

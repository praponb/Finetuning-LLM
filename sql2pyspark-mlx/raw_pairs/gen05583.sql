WITH OrderStats AS (
    SELECT order_id, ROUND(STDDEV(cost), 2) stddev_cost, MAX(cost) max_cost
    FROM shipments
    GROUP BY order_id
)
SELECT o.store_id, O.stddev_cost, O.max_cost
FROM orders o
INNER JOIN OrderStats O ON o.id = O.order_id
WHERE O.stddev_cost > 64
ORDER BY max_cost DESC, 2

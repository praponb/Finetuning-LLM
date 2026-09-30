WITH shipments_by_order AS (
    SELECT order_id, SUM(cost) AS total_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    o.channel,
    sbo.total_cost
FROM orders o
LEFT JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.total_cost >= 170
ORDER BY o.discount, o.channel DESC;

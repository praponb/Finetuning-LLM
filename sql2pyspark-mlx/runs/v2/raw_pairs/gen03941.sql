WITH shipments_by_order AS (
    SELECT order_id, MIN(shipped_date) AS min_shipped_date, AVG(cost) AS avg_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.order_date,
    sbo.min_shipped_date,
    sbo.avg_cost
FROM orders o
JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.avg_cost > 55
ORDER BY sbo.min_shipped_date DESC

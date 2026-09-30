WITH shipments_by_order AS (
    SELECT order_id, MAX(shipped_date) AS latest_shipped_date, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    WHERE order_id IS NOT NULL
    GROUP BY order_id
)
SELECT
    o.shipping_fee,
    o.status,
    sbo.latest_shipped_date,
    sbo.avg_cost
FROM orders o
INNER JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.avg_cost > 33
ORDER BY o.shipping_fee

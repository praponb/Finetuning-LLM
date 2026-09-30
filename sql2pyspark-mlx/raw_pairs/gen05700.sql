WITH shipments_by_order AS (
    SELECT order_id, MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    WHERE MONTH(shipped_date) IN (2, 4, 12)
    GROUP BY order_id
)
SELECT o.store_id, sbo.cost_range
FROM orders o
LEFT JOIN shipments_by_order sbo ON o.id = sbo.order_id
ORDER BY cost_range DESC;

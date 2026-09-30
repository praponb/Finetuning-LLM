WITH order_totals AS (
    SELECT order_id, COUNT(cost) AS cost_count
    FROM shipments
    GROUP BY order_id
)
SELECT o.status, ot.cost_count
FROM orders o
LEFT OUTER JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.cost_count > 8
ORDER BY status DESC

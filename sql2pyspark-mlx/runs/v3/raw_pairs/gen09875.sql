WITH order_totals AS (
    SELECT order_id, SUM(cost) AS total_cost
    FROM shipments
    WHERE order_id = 28
    GROUP BY order_id
)
SELECT o.id, o.customer_id, ot.total_cost
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.id DESC, customer_id DESC

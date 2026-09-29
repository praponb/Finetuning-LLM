WITH order_totals AS (
    SELECT order_id, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.status, ot.avg_amount
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.avg_amount > 750
ORDER BY status, ot.avg_amount DESC

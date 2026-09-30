WITH order_totals AS (
    SELECT order_id, AVG(amount) AS avg_amount
    FROM payments
    WHERE status != 'FAILED'
    GROUP BY order_id
)
SELECT o.amount, ot.avg_amount
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.avg_amount > 410;

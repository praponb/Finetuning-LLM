WITH OrderSummary AS (
    SELECT order_id, MAX(amount) - MIN(amount) AS amount_range
    FROM payments
    WHERE amount < 1330
    GROUP BY order_id
)
SELECT o.order_date, O.amount_range
FROM orders o
INNER JOIN OrderSummary O ON o.id = O.order_id
WHERE O.amount_range >= 54
ORDER BY o.order_date

WITH OrderStats AS (
    SELECT order_id, ROUND(AVG(amount), 2) avg_amount, COUNT(CASE WHEN method = 'cash' THEN 1 END) cash_count
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.channel,
    O.avg_amount,
    O.cash_count
FROM orders o
LEFT JOIN OrderStats O ON o.id = O.order_id
WHERE O.cash_count >= 4

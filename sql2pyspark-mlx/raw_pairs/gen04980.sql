WITH payments_by_order AS (
    SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.order_date, o.discount, pbo.avg_amount
FROM orders o
JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.avg_amount > 28
ORDER BY discount, pbo.avg_amount;

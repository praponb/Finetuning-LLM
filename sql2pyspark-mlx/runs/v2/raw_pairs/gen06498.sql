WITH payments_by_order AS (
    SELECT order_id, MIN(paid_at) AS min_paid_at, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    o.amount,
    pbo.min_paid_at,
    pbo.card_count
FROM orders o
LEFT JOIN payments_by_order pbo ON o.id = pbo.order_id
ORDER BY o.customer_id DESC

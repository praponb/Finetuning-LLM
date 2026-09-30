WITH payments_by_order AS (
    SELECT order_id, MIN(amount) AS min_amount, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
    FROM payments
    WHERE paid_at >= '2024-11-01'
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    pbo.min_amount,
    pbo.failed_count
FROM orders o
LEFT JOIN payments_by_order pbo ON o.id = pbo.order_id
WHERE pbo.min_amount >= 1250

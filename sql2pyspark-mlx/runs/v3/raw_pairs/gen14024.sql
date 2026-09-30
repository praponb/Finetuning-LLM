WITH order_stats AS (
    SELECT order_id, MAX(amount) AS max_amount, SUM(amount) AS total_amount
    FROM payments
    WHERE TO_DATE(paid_at) < '2025-06-01'
    GROUP BY order_id
)
SELECT
    o.id,
    o.status,
    os.max_amount,
    os.total_amount
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id
ORDER BY o.id;

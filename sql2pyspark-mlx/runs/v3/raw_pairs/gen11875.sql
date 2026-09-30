WITH order_stats AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_methods, SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.order_date,
    os.unique_methods,
    os.refunded_amount
FROM orders o
JOIN order_stats os ON o.id = os.order_id
WHERE os.unique_methods >= 4;

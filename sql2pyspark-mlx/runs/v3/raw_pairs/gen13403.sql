WITH order_stats AS (
    SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.store_id, os.avg_amount
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.avg_amount >= 34
ORDER BY os.avg_amount DESC, o.store_id DESC;

WITH order_summary AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_statuss, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    os.unique_statuss,
    os.avg_amount
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id
WHERE os.avg_amount >= 630
ORDER BY o.id DESC;

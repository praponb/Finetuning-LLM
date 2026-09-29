WITH order_summary AS (
    SELECT order_id, COUNT(*) AS cnt, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.shipping_fee,
    os.cnt,
    os.avg_amount
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id
WHERE os.avg_amount >= 65
ORDER BY o.order_date ASC;

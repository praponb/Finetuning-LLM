WITH order_summary AS (
    SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    WHERE status IN ('SUCCESS', 'REFUNDED')
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.discount,
    os.avg_amount
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY os.avg_amount DESC, id
LIMIT 10

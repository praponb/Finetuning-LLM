WITH order_summary AS (
    SELECT order_id, MAX(paid_at) AS latest_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.amount,
    os.latest_paid_at
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY id, order_date
LIMIT 5

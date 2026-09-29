WITH order_summary AS (
    SELECT order_id, MIN(paid_at) AS first_paid_at
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 10 AND 17
    GROUP BY order_id
)
SELECT o.customer_id, os.first_paid_at
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY o.customer_id DESC, first_paid_at;

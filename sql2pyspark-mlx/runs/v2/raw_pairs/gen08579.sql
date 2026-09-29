WITH customer_summary AS (
    SELECT customer_id, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count, COUNT(monthly_fee) AS monthly_fee_count
    FROM subscriptions
    WHERE status IN ('PAUSED', 'CANCELLED', 'EXPIRED')
    GROUP BY customer_id
)
SELECT
    c.is_active,
    c.credit_limit,
    cs.cancelled_count,
    cs.monthly_fee_count
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id

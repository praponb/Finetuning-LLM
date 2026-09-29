WITH customer_summary AS (
    SELECT customer_id, MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.id, c.signup_date, cs.min_monthly_fee
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY min_monthly_fee DESC

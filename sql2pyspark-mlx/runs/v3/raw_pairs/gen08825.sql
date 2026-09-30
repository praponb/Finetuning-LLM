SELECT
    id,
    plan,
    end_date,
    customer_id,
    FIRST_VALUE(customer_id) OVER (PARTITION BY plan ORDER BY end_date, id) AS first_customer_id
FROM subscriptions
WHERE status IN ('CANCELLED', 'EXPIRED')

SELECT
    status,
    plan,
    MIN(monthly_fee) AS min_monthly_fee,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    MIN(end_date) AS min_end_date
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE') AND customer_id IS NOT NULL
GROUP BY status, plan

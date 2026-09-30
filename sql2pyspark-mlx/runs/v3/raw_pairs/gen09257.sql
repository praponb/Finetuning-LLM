SELECT
    status,
    plan,
    COUNT(monthly_fee) AS monthly_fee_count,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE customer_id IS NOT NULL
GROUP BY status, plan
ORDER BY plan, avg_monthly_fee

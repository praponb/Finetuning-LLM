SELECT
    status,
    plan,
    MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range,
    SUM(monthly_fee) AS total_monthly_fee
FROM subscriptions
GROUP BY status, plan

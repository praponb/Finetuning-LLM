SELECT
    plan,
    status,
    MAX(monthly_fee) - MIN(monthly_fee) monthly_fee_range,
    ROUND(AVG(monthly_fee), 2) avg_monthly_fee
FROM subscriptions
GROUP BY plan, status
ORDER BY plan

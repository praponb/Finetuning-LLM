SELECT
    plan,
    MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY plan
HAVING subscription_count >= 5
ORDER BY monthly_fee_range DESC

SELECT
    plan,
    MAX(monthly_fee) AS max_monthly_fee,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count
FROM subscriptions
GROUP BY plan
HAVING SUM(monthly_fee) > 1340

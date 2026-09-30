SELECT
    status,
    plan,
    COUNT(monthly_fee) AS monthly_fee_count,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee
FROM subscriptions
GROUP BY CUBE(status, plan)
ORDER BY basic_monthly_fee DESC;

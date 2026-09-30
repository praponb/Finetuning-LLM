SELECT
    plan,
    CASE WHEN plan IN ('basic', 'enterprise') THEN 'group_a' ELSE 'other' END AS plan_group,
    CASE WHEN monthly_fee >= 214 THEN 'high' WHEN monthly_fee >= 53 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(DISTINCT monthly_fee) AS unique_customers
FROM subscriptions
GROUP BY 1, 2, 3

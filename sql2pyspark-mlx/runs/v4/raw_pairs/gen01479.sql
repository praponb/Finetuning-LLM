SELECT
    CASE WHEN plan IN ('basic', 'standard') THEN 'tier_1' ELSE 'other' END AS plan_group,
    plan,
    ROUND((100 * SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_premium,
    COUNT(DISTINCT monthly_fee) AS unique_customers
FROM subscriptions
WHERE plan != 'standard' AND YEAR(start_date) = 2024
GROUP BY plan_group, plan;

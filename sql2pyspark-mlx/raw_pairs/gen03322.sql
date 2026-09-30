SELECT
    CASE WHEN plan IN ('enterprise', 'premium') THEN 'tier_1' ELSE 'other' END AS plan_group,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
GROUP BY CASE WHEN plan IN ('enterprise', 'premium') THEN 'tier_1' ELSE 'other' END;

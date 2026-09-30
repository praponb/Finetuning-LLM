SELECT
    status,
    CASE WHEN monthly_fee >= 220 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    AVG(monthly_fee) AS avg_monthly_fee,
    SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) AS cancelled_monthly_fee,
    SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'standard' THEN 1 END), 0), 2) AS monthly_fee_per_standard,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles
FROM subscriptions
GROUP BY GROUPING SETS ((status, CASE WHEN monthly_fee >= 220 THEN 'PREMIUM' ELSE 'STANDARD' END), (status))
ORDER BY monthly_fee_tier DESC NULLS LAST

SELECT
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_per_premium,
    PERCENTILE_APPROX(monthly_fee, 0.9) AS monthly_fee_p90,
    GROUPING(DATE_TRUNC('QUARTER', end_date)) AS gid
FROM subscriptions
GROUP BY CUBE (DATE_TRUNC('QUARTER', end_date))
ORDER BY monthly_fee_per_premium DESC NULLS FIRST

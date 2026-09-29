SELECT
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    PERCENTILE_APPROX(monthly_fee, 0.95) AS monthly_fee_p95,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    APPROX_PERCENTILE(monthly_fee, 0.95) AS monthly_fee_p95_2,
    AVG(monthly_fee) AS avg_monthly_fee_2,
    COUNT(*) AS event_count
FROM subscriptions
GROUP BY DATE_TRUNC('QUARTER', start_date)
HAVING APPROX_PERCENTILE(monthly_fee, 0.95) < 1000000;

SELECT
    CASE WHEN monthly_fee >= 210 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    status,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) AS paused_monthly_fee
FROM subscriptions
GROUP BY 1, 2, 3;

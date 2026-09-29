WITH with_windows AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee) AS monthly_fee_rank,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('month', start_date), status ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
)
SELECT
    CASE WHEN monthly_fee_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_rank_tier,
    CASE WHEN monthly_fee_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_bucket_tier,
    DATE_TRUNC('WEEK', start_date) AS week_start,
    COUNT_IF(plan = 'premium') AS premium_events,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee_bucket ELSE 0 END) AS basic_monthly_fee_bucket,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    APPROX_PERCENTILE(monthly_fee_bucket, 0.9) AS monthly_fee_bucket_p90,
    APPROX_PERCENTILE(monthly_fee_rank, 0.5) AS monthly_fee_rank_p50
FROM with_windows
GROUP BY 1, 2, 3
ORDER BY week_start NULLS LAST;

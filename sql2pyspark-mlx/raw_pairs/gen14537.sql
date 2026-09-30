WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        monthly_fee,
        status,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior,
        SUM(monthly_fee) OVER (PARTITION BY DATE_TRUNC('month', start_date), customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        NTILE(5) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        NTILE(3) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket_2
    FROM subscriptions
)
SELECT
    id,
    start_date,
    monthly_fee,
    status,
    monthly_fee_avg_2_prior,
    running_monthly_fee,
    monthly_fee_bucket_2
FROM scored
ORDER BY monthly_fee_avg_2_prior DESC, monthly_fee_bucket_2;

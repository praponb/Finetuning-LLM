WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(start_date, end_date) / 7 AS weeks_end_date_to_start_date,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE end_date < '2024-08-01'
)
SELECT
    CASE WHEN weeks_end_date_to_start_date >= 84 THEN 'LARGE' ELSE 'SMALL' END AS weeks_end_date_to_start_date_tier,
    plan,
    APPROX_PERCENTILE(monthly_fee, 0.9) AS monthly_fee_p90,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee_int ELSE 0 END) AS expired_monthly_fee_int,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    COUNT(*) AS event_count,
    COUNT_IF(plan = 'premium') AS premium_events,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events,
    RANK() OVER (PARTITION BY CASE WHEN weeks_end_date_to_start_date >= 84 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee_int ELSE 0 END) DESC) AS expired_monthly_fee_int_rank
FROM cleaned
GROUP BY CASE WHEN weeks_end_date_to_start_date >= 84 THEN 'LARGE' ELSE 'SMALL' END, plan;

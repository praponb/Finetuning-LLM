WITH enriched AS (
    SELECT
        id,
        start_date,
        end_date,
        status,
        AVG(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM subscriptions
)
SELECT
    id,
    start_date,
    monthly_fee_avg_6_prior,
    cumulative_monthly_fee
FROM enriched;

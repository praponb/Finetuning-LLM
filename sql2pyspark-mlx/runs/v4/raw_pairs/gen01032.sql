SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    SUM(monthly_fee) OVER (PARTITION BY plan, status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee_2,
    RANK() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_rank,
    NTILE(4) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
FROM subscriptions

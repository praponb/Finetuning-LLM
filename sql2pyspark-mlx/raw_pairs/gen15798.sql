WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_TRUNC('month', end_date) AS end_date_period
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    SUM(monthly_fee) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
    ROW_NUMBER() OVER w_grp AS row_seq,
    NTILE(10) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
    LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS prev_monthly_fee,
    SUM(monthly_fee) OVER (ORDER BY end_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
FROM base
WHERE monthly_fee <= 130
WINDOW w_grp AS (PARTITION BY status, customer_id ORDER BY start_date, id);

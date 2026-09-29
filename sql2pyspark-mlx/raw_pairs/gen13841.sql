WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY MONTH(end_date), customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM subscriptions
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    cumulative_monthly_fee
FROM metrics
ORDER BY plan DESC NULLS FIRST, monthly_fee DESC

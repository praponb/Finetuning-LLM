WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(monthly_fee_int) OVER (PARTITION BY status, customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_monthly_fee_int,
        SUM(monthly_fee_int) OVER (PARTITION BY status, customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_monthly_fee_int,
        MAX(monthly_fee_int) OVER (PARTITION BY status, customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_monthly_fee_int,
        NTILE(5) OVER (PARTITION BY plan, status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
    FROM base
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_int,
        last3_avg_monthly_fee_int,
        last3_sum_monthly_fee_int,
        monthly_fee_bucket,
        CONCAT_WS(',', status, plan) AS status_plan_key
    FROM scored
    WHERE start_date > DATE '2023-03-28'
      AND last3_max_monthly_fee_int > 78
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last3_sum_monthly_fee_int) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last3_sum_monthly_fee_int,
        AVG(last3_sum_monthly_fee_int) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last3_sum_monthly_fee_int,
        MAX(last3_sum_monthly_fee_int) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last3_sum_monthly_fee_int
    FROM base_subscriptions
)
-- Join a dimension and aggregate
SELECT
    c.city,
    src.plan,
    COUNT(*) AS rows_total,
    ROUND(AVG(src.monthly_fee_int), 2) AS avg_monthly_fee_int,
    AVG(src.running_max_last3_sum_monthly_fee_int) AS avg_running_max_last3_sum_monthly_fee_int,
    GROUPING_ID() AS level_id
FROM with_windows src
LEFT JOIN customers c ON src.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, src.plan), (c.city))
ORDER BY avg_running_max_last3_sum_monthly_fee_int DESC;

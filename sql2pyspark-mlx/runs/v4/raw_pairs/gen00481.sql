WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        ROUND(monthly_fee * 0.85, 2) AS monthly_fee_adjusted
    FROM subscriptions
    WHERE end_date >= DATE '2023-05-05'
      AND status = 'ACTIVE'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        monthly_fee_adjusted,
        rn,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY monthly_fee_adjusted DESC NULLS FIRST) AS monthly_fee_adjusted_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_adjusted DESC NULLS FIRST) AS monthly_fee_adjusted_dense_rank,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee_adjusted, id) AS monthly_fee_adjusted_bucket,
        SUM(monthly_fee_adjusted) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_adjusted
    FROM latest
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(monthly_fee_adjusted_dense_rank * 100, 2) AS adjusted_monthly_fee_adjusted_dense_rank
    FROM metrics
    WHERE status IN ('EXPIRED', 'PAUSED')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        monthly_fee_adjusted,
        rn,
        monthly_fee_adjusted_percent_rank,
        monthly_fee_adjusted_dense_rank,
        running_monthly_fee_adjusted,
        adjusted_monthly_fee_adjusted_dense_rank,
        COALESCE(monthly_fee_adjusted, 0) AS monthly_fee_adjusted_filled,
        monthly_fee_adjusted_dense_rank + monthly_fee_adjusted_bucket AS monthly_fee_adjusted_dense_rank_plus_monthly_fee_adjusted_bucket
    FROM valid
    WHERE start_date > DATE '2023-09-08'
      AND start_date BETWEEN '2023-10-01' AND '2025-08-01'
),
latest_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY adjusted_monthly_fee_adjusted_dense_rank, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee_adjusted_filled >= 69 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_adjusted_filled_tier,
    CASE WHEN monthly_fee_adjusted_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_adjusted_dense_rank_tier,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(DISTINCT plan) AS distinct_plans,
    RANK() OVER (PARTITION BY CASE WHEN monthly_fee_adjusted_filled >= 69 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) FILTER (WHERE plan = 'basic') DESC) AS basic_rows_rank
FROM latest_subscriptions
GROUP BY CASE WHEN monthly_fee_adjusted_filled >= 69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN monthly_fee_adjusted_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END
HAVING basic_rows >= 1
ORDER BY monthly_fee_adjusted_dense_rank_tier;

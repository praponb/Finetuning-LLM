WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-15'
      AND customer_id IS NOT NULL
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN monthly_fee_rounded >= 81 THEN 'High' WHEN monthly_fee_rounded >= 28 THEN 'Medium' ELSE 'Low' END AS monthly_fee_rounded_tier
    FROM base
    WHERE start_date > TO_DATE('2024-03-28')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        monthly_fee_rounded,
        monthly_fee_rounded_tier,
        SUM(monthly_fee_rounded) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_rounded,
        LEAD(monthly_fee_rounded, 1) OVER (ORDER BY start_date ASC, id ASC) AS next_monthly_fee_rounded,
        MAX(monthly_fee_rounded) OVER (PARTITION BY status, plan) AS status_max_monthly_fee_rounded,
        NTILE(5) OVER (PARTITION BY status ORDER BY monthly_fee_rounded ASC, id ASC) AS monthly_fee_rounded_bucket,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_rounded ASC) AS monthly_fee_rounded_percent_rank
    FROM base_subscriptions
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        status,
        monthly_fee_rounded_tier,
        running_monthly_fee_rounded,
        status_max_monthly_fee_rounded,
        monthly_fee_rounded_bucket,
        monthly_fee_rounded_percent_rank,
        INITCAP(TRIM(status)) AS status_title,
        monthly_fee_rounded_percent_rank + monthly_fee_rounded_bucket AS monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket
    FROM with_windows
    WHERE start_date >= DATE '2023-11-13'
      AND monthly_fee_rounded_bucket >= 4
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY start_date ASC NULLS LAST, id ASC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        monthly_fee_rounded_tier,
        monthly_fee_rounded_bucket,
        monthly_fee_rounded_percent_rank,
        monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket,
        rn,
        monthly_fee_rounded_percent_rank - monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket AS monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket,
        LOWER(plan) AS plan_lower
    FROM latest
    WHERE status_max_monthly_fee_rounded BETWEEN 37 AND 77
),
with_windows_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket, 1) OVER (ORDER BY start_date ASC, id ASC) AS prev_monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket,
        FIRST_VALUE(monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket, TRUE) OVER (ORDER BY monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_rounded_percent_rank_minus_monthly_fee_rounded_percent_rank_plus_monthly_fee_rounded_bucket,
        LAST_VALUE(monthly_fee_rounded_percent_rank) IGNORE NULLS OVER (ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_rounded_percent_rank,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_rounded_percent_rank DESC NULLS LAST) AS monthly_fee_rounded_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_rounded_bucket DESC NULLS FIRST) AS monthly_fee_rounded_bucket_percent_rank
    FROM valid
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last_known_monthly_fee_rounded_percent_rank >= 0.75 THEN 'HIGH' ELSE 'LOW' END AS last_known_monthly_fee_rounded_percent_rank_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(monthly_fee_rounded_percent_rank_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_rounded_percent_rank_percent_rank_quartiles,
        COUNT(DISTINCT id) AS distinct_ids_2,
        APPROX_PERCENTILE(monthly_fee_rounded_percent_rank_percent_rank, 0.9) AS monthly_fee_rounded_percent_rank_percent_rank_p90
    FROM with_windows_subscriptions
    GROUP BY last_known_monthly_fee_rounded_percent_rank_tier
    HAVING COUNT(DISTINCT id) > 1
)
SELECT
    last_known_monthly_fee_rounded_percent_rank_tier,
    distinct_ids,
    monthly_fee_rounded_percent_rank_percent_rank_quartiles,
    distinct_ids_2,
    monthly_fee_rounded_percent_rank_percent_rank_p90
FROM daily
ORDER BY last_known_monthly_fee_rounded_percent_rank_tier DESC NULLS LAST;

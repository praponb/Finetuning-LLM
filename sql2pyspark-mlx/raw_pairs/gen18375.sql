WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date, id) AS dedup_rank
    FROM subscriptions
    QUALIFY dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        dedup_rank + monthly_fee dedup_rank_plus_monthly_fee
    FROM ranked
    WHERE customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY dedup_rank_plus_monthly_fee, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
ranked_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        latest_rank
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY latest_rank DESC, id) = 1
)
-- Multi-level aggregation
SELECT
    plan,
    CASE WHEN monthly_fee >= 216 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    MAX(monthly_fee) AS max_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS monthly_fee_per_cancelled,
    AVG(monthly_fee) FILTER (WHERE status = 'CANCELLED') AS cancelled_avg_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'standard' THEN 1 END), 0), 2) AS monthly_fee_per_standard,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events,
    GROUPING(plan) AS grouping_id
FROM ranked_subscriptions
GROUP BY CUBE (plan, CASE WHEN monthly_fee >= 216 THEN 'PREMIUM' ELSE 'STANDARD' END)

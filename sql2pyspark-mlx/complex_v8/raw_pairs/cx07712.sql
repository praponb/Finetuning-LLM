WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE risk_score > 0.3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        region,
        segment,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY region)) AS distinct_segment_in_region,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_joined_on NULLS LAST), 3) AS days_since_joined_on_percent_rank,
        days_since_joined_on - LAG(days_since_joined_on) OVER w_all_by_days_since_joined_on_2 AS days_since_joined_on_change,
        MIN(days_since_joined_on) OVER (PARTITION BY segment, region) AS segment_region_min_days_since_joined_on,
        LAG(joined_on) OVER w_all_by_days_since_joined_on_3 AS prev_joined_on,
        RANK() OVER (ORDER BY days_since_joined_on NULLS LAST, id) AS days_since_joined_on_rank,
        MAX(days_since_joined_on) OVER (ORDER BY joined_on DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_joined_on_max_3
    FROM base
    WINDOW w_all_by_days_since_joined_on_2 AS (ORDER BY days_since_joined_on NULLS LAST, id), w_all_by_days_since_joined_on_3 AS (ORDER BY days_since_joined_on DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        holder_name,
        region,
        segment,
        distinct_segment_in_region,
        days_since_joined_on_percent_rank,
        days_since_joined_on_change,
        segment_region_min_days_since_joined_on,
        prev_joined_on,
        days_since_joined_on_max_3
    FROM windowed
    WHERE days_since_joined_on_rank = 1
)
SELECT
    id,
    holder_name,
    segment,
    segment_region_min_days_since_joined_on,
    prev_joined_on,
    days_since_joined_on_max_3,
    DATE_ADD(prev_joined_on, 365) AS prev_joined_on_plus_365d,
    DATEDIFF(DATE '2026-01-01', prev_joined_on) AS days_since_prev_joined_on
FROM leaders
ORDER BY days_since_prev_joined_on

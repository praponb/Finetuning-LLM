WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        currency,
        tier_value,
        ROUND(tier_value * 0.85, 2) adjusted_tier_value
    FROM flattened
    WHERE effective_from >= DATE '2024-04-08'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) tier_value_rank,
        DENSE_RANK() OVER (ORDER BY adjusted_tier_value DESC) adjusted_tier_value_dense_rank
    FROM filtered
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        tier_value,
        tier_value_rank,
        adjusted_tier_value_dense_rank,
        CEIL(tier_value) tier_value_whole
    FROM scored
    WHERE currency IN ('EUR', 'THB')
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(adjusted_tier_value_dense_rank) OVER (PARTITION BY plan) plan_stddev_samp_adjusted_tier_value_dense_rank,
        ROUND(adjusted_tier_value_dense_rank / SUM(adjusted_tier_value_dense_rank) OVER (PARTITION BY plan), 4) adjusted_tier_value_dense_rank_share,
        MAX(adjusted_tier_value_dense_rank) OVER (PARTITION BY plan) plan_max_adjusted_tier_value_dense_rank,
        ROUND(adjusted_tier_value_dense_rank / SUM(adjusted_tier_value_dense_rank) OVER (PARTITION BY plan), 4) adjusted_tier_value_dense_rank_share_2
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_stddev_samp_adjusted_tier_value_dense_rank DESC) plan_stddev_samp_adjusted_tier_value_dense_rank_percent_rank
    FROM scored_price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_tier_value_dense_rank_share >= 0.74 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_tier_value_dense_rank_share_tier,
    plan,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    SUM(tier_value_rank) FILTER (WHERE plan = 'starter') AS starter_total_tier_value_rank,
    GROUPING_ID() AS grouping_id
FROM metrics
GROUP BY ROLLUP (CASE WHEN adjusted_tier_value_dense_rank_share >= 0.74 THEN 'LARGE' ELSE 'SMALL' END, plan)
ORDER BY pro_count DESC

WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        tier_value,
        MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value
    FROM lines
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        tier_value,
        plan_max_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist_2,
        PERCENT_RANK() OVER (ORDER BY plan_max_tier_value DESC NULLS FIRST) AS plan_max_tier_value_percent_rank,
        DENSE_RANK() OVER (ORDER BY plan_max_tier_value DESC NULLS LAST) AS plan_max_tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist_3
    FROM scored
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        plan_max_tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        plan_max_tier_value_percent_rank,
        plan_max_tier_value_dense_rank,
        tier_value_cume_dist_3,
        plan_max_tier_value_percent_rank + tier_value_cume_dist_3 AS plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3,
        ROUND(plan_max_tier_value_percent_rank / plan_max_tier_value_dense_rank, 2) AS plan_max_tier_value_percent_rank_per_plan_max_tier_value_dense_rank
    FROM enriched
    WHERE plan_max_tier_value <= 44
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        plan_max_tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        plan_max_tier_value_percent_rank,
        plan_max_tier_value_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_max_tier_value_dense_rank DESC NULLS LAST) AS plan_max_tier_value_dense_rank_percent_rank,
        RANK() OVER (ORDER BY plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3 DESC NULLS LAST) AS plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3_rank
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(tier_value_cume_dist_2) AS tier_value_cume_dist_2_whole
    FROM metrics
    WHERE plan_max_tier_value_percent_rank IS NULL
)
SELECT
    plan_max_tier_value,
    tier_value_cume_dist,
    tier_value_cume_dist_2,
    plan_max_tier_value_dense_rank,
    plan_max_tier_value_dense_rank_percent_rank,
    plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3_rank
FROM cleaned;

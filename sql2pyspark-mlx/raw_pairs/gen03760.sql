WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        SUM(tier_value) OVER (PARTITION BY plan) plan_sum_tier_value,
        CUME_DIST() OVER (ORDER BY tier_value) tier_value_cume_dist
    FROM exploded
    WHERE plan = 'starter'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(plan) clean_plan
    FROM enriched
    WHERE plan_sum_tier_value < 326
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(tier_value_cume_dist, 0) tier_value_cume_dist_filled
    FROM base
    WHERE plan_sum_tier_value < 118
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN tier_value_cume_dist >= 0.61 THEN 'High' WHEN tier_value_cume_dist >= 0.14 THEN 'Medium' ELSE 'Low' END tier_value_cume_dist_level
    FROM filtered
    WHERE tier_value_cume_dist_filled <= 48
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN tier_value_cume_dist >= 0.82 THEN 'High' WHEN tier_value_cume_dist >= 0.27 THEN 'Medium' ELSE 'Low' END tier_value_cume_dist_level_2
    FROM cleaned
    WHERE tier_value_cume_dist BETWEEN 0.39 AND 0.44
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(tier_value_cume_dist_filled AS INT) tier_value_cume_dist_filled_int
    FROM valid
    WHERE plan_sum_tier_value <= 158
)
SELECT
    plan_sum_tier_value,
    tier_value_cume_dist,
    clean_plan,
    tier_value_cume_dist_filled,
    tier_value_cume_dist_level,
    tier_value_cume_dist_filled_int
FROM cleaned_price_tiers
ORDER BY plan_sum_tier_value DESC, tier_value_cume_dist_filled_int;

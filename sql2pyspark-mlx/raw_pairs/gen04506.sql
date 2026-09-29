WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(tier_value AS DOUBLE) AS tier_value_num
    FROM lines
    WHERE plan IN ('pro', 'enterprise', 'basic')
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, AVG(tier_value) OVER (PARTITION BY currency, plan) AS currency_avg_tier_value
    FROM valid
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        currency,
        tier_value,
        tier_value_num,
        currency_avg_tier_value,
        ROUND(tier_value * 1.07, 2) AS adjusted_tier_value,
        LOWER(plan) AS plan_lower
    FROM with_windows
    WHERE currency NOT IN ('EUR', 'USD')
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(adjusted_tier_value) OVER (PARTITION BY currency) AS currency_count_adjusted_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY adjusted_tier_value DESC NULLS FIRST) AS adjusted_tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_cume_dist
    FROM base
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(adjusted_tier_value, 1) AS adjusted_tier_value_rounded
    FROM with_windows_price_tiers
    WHERE currency != 'THB'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        tier_value,
        tier_value_num,
        currency_avg_tier_value,
        currency_count_adjusted_tier_value,
        adjusted_tier_value_dense_rank,
        COUNT(adjusted_tier_value_dense_rank) OVER (PARTITION BY currency) AS currency_count_adjusted_tier_value_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY adjusted_tier_value DESC) AS adjusted_tier_value_rank,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist NULLS FIRST) AS tier_value_cume_dist_rank
    FROM filtered
)
SELECT
    tier_value_num,
    adjusted_tier_value_dense_rank,
    currency_count_adjusted_tier_value_dense_rank,
    adjusted_tier_value_rank,
    tier_value_cume_dist_rank
FROM metrics;

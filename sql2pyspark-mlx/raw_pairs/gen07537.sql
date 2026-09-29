WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN plan IN ('basic', 'starter') THEN 'Core' ELSE 'Other' END AS plan_group,
        CAST(tier_value AS INT) AS tier_value_int
    FROM lines
    WHERE tier_value >= 72
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        plan,
        currency,
        tier_value,
        plan_group,
        PERCENT_RANK() OVER (PARTITION BY currency, plan ORDER BY tier_value_int DESC NULLS FIRST) AS tier_value_int_percent_rank,
        MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value
    FROM filtered
    WHERE plan = 'enterprise'
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        tier_value,
        plan_group,
        plan_max_tier_value,
        tier_value / NULLIF(plan_max_tier_value, 0) AS tier_value_per_plan_max_tier_value
    FROM with_windows
    WHERE tier_value >= 39
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(tier_value_per_plan_max_tier_value, 0) AS tier_value_per_plan_max_tier_value_rounded,
        plan_max_tier_value * tier_value_per_plan_max_tier_value AS plan_max_tier_value_times_tier_value_per_plan_max_tier_value
    FROM filtered_price_tiers
    WHERE tier_value_per_plan_max_tier_value > 79
)
SELECT
    tier_value,
    plan_group,
    tier_value_per_plan_max_tier_value_rounded,
    plan_max_tier_value_times_tier_value_per_plan_max_tier_value
FROM valid

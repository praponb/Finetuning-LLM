WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(tier_value AS INT) tier_value_int,
        INITCAP(TRIM(plan)) plan_title
    FROM exploded
    WHERE tier_value > 28
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        tier_value,
        tier_value_int,
        plan_title,
        CASE WHEN tier_value >= 87 THEN 'High' WHEN tier_value >= 39 THEN 'Medium' ELSE 'Low' END tier_value_band
    FROM valid
    WHERE tier_value <= 65
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        tier_value,
        tier_value_int,
        GREATEST(tier_value_int, tier_value) greatest_tier_value_int_tier_value
    FROM valid_price_tiers
    WHERE plan IN ('basic', 'pro')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(tier_value, greatest_tier_value_int_tier_value) greatest_tier_value_greatest_tier_value_int_tier_value
    FROM filtered
    WHERE tier_value_int >= 71
)
SELECT tier_value, greatest_tier_value_int_tier_value
FROM base
ORDER BY tier_value NULLS LAST;

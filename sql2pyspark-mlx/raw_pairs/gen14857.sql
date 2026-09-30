WITH flattened AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
base AS (
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        CASE WHEN tier_value >= 82 THEN 'High' WHEN tier_value >= 16 THEN 'Medium' ELSE 'Low' END AS tier_value_tier
    FROM flattened
    WHERE tier_value >= 40
),
valid AS (
    SELECT
        *,
        CASE WHEN plan_length >= 62 THEN 'High' WHEN plan_length >= 10 THEN 'Medium' ELSE 'Low' END AS plan_length_tier,
        tier_value / NULLIF(plan_length, 0) AS tier_value_per_plan_length
    FROM base
    WHERE tier_value > 47
),
base_price_tiers AS (
    SELECT
        id,
        plan,
        tier_value,
        plan_length,
        tier_value_tier,
        plan_length_tier,
        tier_value_per_plan_length,
        tier_value * plan_length AS tier_value_times_plan_length
    FROM valid
    WHERE currency IN ('THB', 'USD', 'EUR')
),
valid_price_tiers AS (
    SELECT *, ROUND(tier_value_times_plan_length * 0.9, 2) AS tier_value_times_plan_length_scaled
    FROM base_price_tiers
    WHERE tier_value_times_plan_length >= 58
)
SELECT
    plan,
    plan_length,
    tier_value_tier,
    plan_length_tier,
    tier_value_per_plan_length,
    tier_value_times_plan_length,
    tier_value_times_plan_length_scaled
FROM valid_price_tiers
ORDER BY tier_value_times_plan_length ASC NULLS FIRST

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        UPPER(currency) AS currency_upper
    FROM exploded
    WHERE currency IN ('USD', 'EUR', 'THB')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', currency, plan) AS currency_plan_key,
        ROUND(tier_value, 0) AS tier_value_rounded
    FROM valid
    WHERE plan != 'enterprise'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(tier_value_rounded, 2) AS tier_value_rounded_rounded,
        tier_value * tier_value_rounded AS tier_value_times_tier_value_rounded
    FROM cleaned
    WHERE currency IN ('EUR', 'THB')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN tier_value_rounded_rounded >= 74 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_rounded_rounded_tier,
        SUM(CASE WHEN currency = 'THB' THEN tier_value_rounded ELSE 0 END) AS thb_tier_value_rounded,
        PERCENTILE_APPROX(tier_value_times_tier_value_rounded, ARRAY(0.25, 0.5, 0.75)) AS tier_value_times_tier_value_rounded_quartiles,
        COUNT_IF(currency = 'EUR') AS eur_events
    FROM base
    GROUP BY tier_value_rounded_rounded_tier
)
SELECT
    tier_value_rounded_rounded_tier,
    thb_tier_value_rounded,
    tier_value_times_tier_value_rounded_quartiles
FROM totals

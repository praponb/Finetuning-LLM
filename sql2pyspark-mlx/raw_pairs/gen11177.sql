WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
),
filtered AS (
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM exploded
    WHERE currency = 'THB'
),
filtered_price_tiers AS (
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_title,
        CASE WHEN currency = 'THB' THEN 1 ELSE 0 END AS is_thb
    FROM filtered
    WHERE tier_value >= 22
),
valid AS (
    SELECT
        id,
        tier_value,
        currency_title,
        is_thb,
        is_thb / NULLIF(tier_value, 0) AS is_thb_per_tier_value
    FROM filtered_price_tiers
    WHERE plan = 'pro'
),
valid_price_tiers AS (
    SELECT *, GREATEST(is_thb_per_tier_value, tier_value) AS greatest_is_thb_per_tier_value_tier_value
    FROM valid
    WHERE is_thb BETWEEN 54 AND 69
)
SELECT
    CASE WHEN greatest_is_thb_per_tier_value_tier_value >= 72 THEN 'HIGH' ELSE 'LOW' END AS greatest_is_thb_per_tier_value_tier_value_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(is_thb, ARRAY(0.25, 0.5, 0.75)) AS is_thb_quartiles,
    MAX(is_thb_per_tier_value) AS max_is_thb_per_tier_value
FROM valid_price_tiers
GROUP BY CASE WHEN greatest_is_thb_per_tier_value_tier_value >= 72 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids DESC;

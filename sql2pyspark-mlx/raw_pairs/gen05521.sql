WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
)
SELECT
    id,
    plan,
    currency,
    tier_value,
    CASE WHEN currency RLIKE '^(EUR|USD)$' THEN 'OTHER_ZONE' WHEN currency RLIKE '^(THB)$' THEN 'GROUP_A' ELSE 'CORE' END AS currency_zone
FROM flattened
ORDER BY currency_zone DESC NULLS LAST

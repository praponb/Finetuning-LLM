WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    plan,
    tier_value
FROM flattened
ORDER BY plan DESC NULLS FIRST

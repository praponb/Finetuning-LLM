WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
)
SELECT
    plan,
    currency,
    val
FROM exploded
ORDER BY val DESC, currency DESC

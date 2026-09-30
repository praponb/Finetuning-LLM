WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
)
SELECT effective_from
FROM flattened
ORDER BY effective_from NULLS LAST

WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS tier_value
)
SELECT id, tier_value
FROM flattened
ORDER BY id DESC NULLS LAST;

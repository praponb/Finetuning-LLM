WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    tier_value
FROM flattened
ORDER BY tier_value ASC NULLS FIRST, effective_from ASC;

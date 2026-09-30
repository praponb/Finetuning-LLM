WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT id, effective_from
FROM flattened
ORDER BY effective_from DESC NULLS FIRST

WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
)
SELECT effective_from, element
FROM exploded
ORDER BY effective_from NULLS FIRST

WITH exploded AS (
    SELECT id, element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
)
SELECT id, element
FROM exploded
ORDER BY element ASC NULLS FIRST

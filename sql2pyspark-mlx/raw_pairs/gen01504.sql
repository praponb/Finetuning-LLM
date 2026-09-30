WITH lines AS (
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS element
)
SELECT id, effective_from
FROM lines
ORDER BY id

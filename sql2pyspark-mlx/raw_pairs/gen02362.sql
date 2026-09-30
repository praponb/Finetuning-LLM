WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
)
SELECT
    id,
    plan,
    element,
    CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY element NULLS LAST) AS element_cume_dist
FROM lines
WHERE element < 41

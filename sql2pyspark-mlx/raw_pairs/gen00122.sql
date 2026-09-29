WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS element
)
SELECT
    effective_from,
    element,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY element) AS element_cume_dist
FROM lines

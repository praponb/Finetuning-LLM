WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS val
)
SELECT
    currency,
    CASE WHEN val >= 63 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    MAX(val) FILTER (WHERE currency = 'THB') AS thb_max_val,
    MAX(val) AS max_val,
    SUM(CASE WHEN currency = 'THB' THEN val ELSE 0 END) AS thb_val
FROM exploded
GROUP BY GROUPING SETS ((currency, CASE WHEN val >= 63 THEN 'LARGE' ELSE 'SMALL' END), (currency))
ORDER BY val_tier DESC NULLS LAST;

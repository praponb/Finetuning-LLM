WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
)
SELECT
    currency,
    SUM(CASE WHEN currency = 'THB' THEN element ELSE 0 END) AS thb_element,
    ROUND(AVG(element), 2) AS avg_element,
    MIN(element) AS min_element
FROM exploded
GROUP BY currency
HAVING SUM(CASE WHEN currency = 'THB' THEN element ELSE 0 END) < 1000000
ORDER BY min_element DESC;

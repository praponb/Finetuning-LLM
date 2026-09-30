WITH flattened AS (
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS element
)
SELECT
    CASE WHEN element >= 79 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    currency,
    COUNT(DISTINCT currency) AS distinct_currencies,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    ROUND(AVG(element), 2) AS avg_element,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(*) AS cnt,
    COUNT_IF(currency = 'EUR') AS eur_events
FROM flattened
GROUP BY CASE WHEN element >= 79 THEN 'LARGE' ELSE 'SMALL' END, currency
HAVING SIZE(COLLECT_SET(currency)) > 1;

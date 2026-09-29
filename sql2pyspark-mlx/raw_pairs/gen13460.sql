WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS element
)
SELECT
    CASE WHEN element >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_tier,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT_IF(plan = 'basic') AS basic_events,
    COUNT_IF(plan = 'enterprise') AS enterprise_events
FROM lines
GROUP BY CASE WHEN element >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', effective_from)
HAVING basic_events < 1000
ORDER BY basic_events NULLS LAST;

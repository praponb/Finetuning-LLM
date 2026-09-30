WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT *, ADD_MONTHS(effective_from, 6) AS effective_from_plus_months
    FROM flattened
    WHERE plan = 'starter'
),
filtered_price_tiers AS (
    SELECT *, CASE WHEN currency IN ('USD', 'EUR') THEN 'Group A' ELSE 'Other' END AS currency_group
    FROM filtered
    WHERE effective_from > TO_DATE('2024-06-09')
)
SELECT
    CASE WHEN tier_value >= 61 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    DATE_TRUNC('WEEK', effective_from_plus_months) AS week_start,
    COUNT_IF(plan = 'starter') AS starter_events,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) AS cnt,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(*) AS event_count
FROM filtered_price_tiers
GROUP BY ROLLUP (CASE WHEN tier_value >= 61 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', effective_from_plus_months))
ORDER BY distinct_plans DESC NULLS LAST

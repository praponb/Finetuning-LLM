WITH exploded AS (
    SELECT
        id,
        plan,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    CASE WHEN tier_value >= 80 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    plan,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM exploded
GROUP BY 1, 2
HAVING cnt > 1

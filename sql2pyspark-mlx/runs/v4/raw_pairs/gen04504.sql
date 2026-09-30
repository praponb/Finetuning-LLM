WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM flattened
GROUP BY quarter_start;

WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    plan,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(*) AS num_rows,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS tier_value_per_enterprise,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows_2,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    GROUPING(DATE_TRUNC('WEEK', effective_from)) AS grouping_id
FROM exploded
GROUP BY ROLLUP (DATE_TRUNC('WEEK', effective_from), plan)
ORDER BY basic_rows DESC;

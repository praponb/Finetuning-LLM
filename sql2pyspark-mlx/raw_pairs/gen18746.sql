WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS element
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    plan,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    ROUND(AVG(element), 2) AS avg_element,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    GROUPING(DATE_TRUNC('QUARTER', effective_from)) AS grouping_id
FROM exploded
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('MONTH', effective_from), plan), (DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('MONTH', effective_from)), ())
ORDER BY basic_count DESC

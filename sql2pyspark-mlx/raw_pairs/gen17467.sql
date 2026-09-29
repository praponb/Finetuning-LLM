WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    currency,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM exploded
GROUP BY DATE_TRUNC('MONTH', effective_from), currency
HAVING distinct_ids > 1
ORDER BY starter_rows DESC;

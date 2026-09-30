WITH filtered_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE category <> 'Beauty'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        category,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered_skus
    GROUP BY category
    HAVING SUM(unit_cost) > 108.5
)
-- Final projection
SELECT
    category,
    category_list,
    COALESCE(category, 'UNKNOWN') AS category_or_unknown,
    COALESCE(distinct_ids, 0) AS distinct_ids_filled
FROM rollup_base
WHERE category = 'Food'
ORDER BY category_or_unknown DESC, category
LIMIT 5

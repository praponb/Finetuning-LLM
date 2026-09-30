WITH filtered_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        labels,
        is_active
    FROM skus
    WHERE weight_kg < 14.6
      AND UPPER(TRIM(sku_code)) LIKE 'E%'
),
per_group AS (
    -- Summarise per group
    SELECT
        category,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        COUNT(DISTINCT category) AS distinct_categorys,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered_skus
    GROUP BY 1
),
exploded AS (
    -- One row per sku_code_list_item
    SELECT
        s.category,
        s.category_list,
        s.distinct_categorys,
        s.n_rows,
        s.distinct_ids,
        t.sku_code_list_item
    FROM per_group s
    LATERAL VIEW EXPLODE(s.sku_code_list) t AS sku_code_list_item
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_categorys, sku_code_list_item, category, n_rows
    FROM exploded
) src
PIVOT (
    MAX(n_rows)
    FOR category IN ('Electronics', 'Beauty')
)

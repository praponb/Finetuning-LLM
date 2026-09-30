WITH cleaned AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        ARRAY_CONTAINS(labels, 'fragile') AS has_fragile,
        CONCAT_WS('-', sku_code, category) AS sku_code_category_label,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref
    FROM skus
    WHERE (weight_kg > 8.5 OR NOT ARRAY_CONTAINS(labels, 'fragile'))
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.unit_price < 292.3
        )
),
filtered_2 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id >= 2
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        has_fragile,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        COUNT(DISTINCT category) AS distinct_categorys,
        ROUND(SUM(weight_kg), 2) AS total_weight_kg,
        ROUND(100 * COUNT(CASE WHEN category IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_category_is_not_null
    FROM filtered_2
    GROUP BY 1, 2
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE pct_category_is_not_null > (SELECT MIN(pct_category_is_not_null) FROM stats)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE distinct_categorys < (SELECT MAX(distinct_categorys) FROM with_activity)
),
enriched AS (
    -- Label and score rows
    SELECT
        category,
        has_fragile,
        category_list,
        total_weight_kg,
        pct_category_is_not_null,
        CASE WHEN pct_category_is_not_null >= 85.0 THEN 'large' WHEN pct_category_is_not_null >= 45.0 THEN 'medium' ELSE 'small' END AS pct_category_is_not_null_tier,
        CASE WHEN total_weight_kg >= 112.5 THEN 'A' WHEN total_weight_kg >= 52.6 THEN 'B' ELSE 'C' END AS total_weight_kg_tier
    FROM qualified
)
SELECT
    category,
    category_list,
    total_weight_kg,
    pct_category_is_not_null,
    pct_category_is_not_null_tier,
    total_weight_kg_tier,
    LOWER(REGEXP_REPLACE(TRIM(total_weight_kg_tier), ' ', '_')) AS total_weight_kg_tier_slug,
    SIZE(category_list) AS category_list_count
FROM enriched

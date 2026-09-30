WITH recent_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ROUND(weight_kg * 1.5, 2) AS weight_kg_adj,
        CONCAT_WS('|', sku_code, category) AS sku_code_category_label
    FROM skus
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_skus
    WHERE unit_cost > (SELECT AVG(unit_cost) FROM recent_skus)
)
-- Subtotals with GROUPING SETS
SELECT
    is_active,
    COALESCE(category, 'ALL') AS category,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows
FROM qualified
GROUP BY GROUPING SETS ((is_active, category), (is_active))
ORDER BY is_active NULLS LAST, category NULLS LAST

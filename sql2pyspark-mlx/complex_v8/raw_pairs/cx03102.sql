WITH filtered AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        weight_kg,
        labels,
        is_active,
        INITCAP(TRIM(sku_code)) AS sku_code_clean,
        CASE WHEN category IN ('Beauty', 'Electronics') THEN 'group_a' WHEN category = 'Toys' THEN 'group_b' ELSE 'other' END AS category_group,
        GREATEST(unit_cost, weight_kg) AS greatest_unit_cost_weight_kg
    FROM skus
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.moved_at >= TIMESTAMP '2025-01-31 10:00:00'
        )
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE greatest_unit_cost_weight_kg > (SELECT AVG(greatest_unit_cost_weight_kg) FROM without_matches)
)
SELECT
    category_group,
    is_active,
    category,
    GROUPING(category_group) AS is_total_category_group,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT category) AS distinct_categorys
FROM screened
GROUP BY CUBE(category_group, is_active, category)
ORDER BY category_group NULLS LAST, is_active NULLS LAST, category NULLS LAST

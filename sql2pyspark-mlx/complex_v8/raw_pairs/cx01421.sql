WITH scoped_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ARRAY_JOIN(ARRAY_SORT(labels), ',') AS labels_text,
        COALESCE(unit_cost, 0.0) AS unit_cost_filled,
        ABS(weight_kg - 6.5) AS weight_kg_distance
    FROM skus
    WHERE LENGTH(TRIM(sku_code)) >= 7
),
enriched AS (
    -- Label and score rows
    SELECT *, GREATEST(unit_cost, weight_kg) AS greatest_unit_cost_weight_kg
    FROM scoped_skus
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
        )
)
-- Subtotals with CUBE
SELECT
    COALESCE(category, 'ALL') AS category,
    is_active,
    MIN(weight_kg_distance) AS min_weight_kg_distance,
    SUM(CASE WHEN category = 'Electronics' THEN unit_cost ELSE 0 END) AS unit_cost_category_electronics
FROM without_matches
GROUP BY CUBE(category, is_active)
ORDER BY category NULLS LAST, is_active NULLS LAST

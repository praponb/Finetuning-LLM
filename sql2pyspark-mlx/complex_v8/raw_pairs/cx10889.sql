WITH prepared_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        CONCAT_WS(': ', category, sku_code) AS category_sku_code_label
    FROM skus
    WHERE sku_code IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        category_sku_code_label,
        LOWER(TRIM(category)) AS category_clean
    FROM prepared_skus
    WHERE category_sku_code_label IS NOT NULL
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.unit_price > 206.8
              AND st.move_type IN ('TRANSFER', 'OUTBOUND', 'INBOUND')
        )
)
-- Final projection
SELECT
    id,
    unit_cost,
    weight_kg,
    category_sku_code_label,
    category_clean,
    LOWER(TRIM(category_sku_code_label)) AS category_sku_code_label_clean,
    ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg
FROM qualified
ORDER BY category_sku_code_label_clean DESC NULLS FIRST

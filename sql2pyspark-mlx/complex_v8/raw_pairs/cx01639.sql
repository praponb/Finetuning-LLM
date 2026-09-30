WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE is_active = FALSE
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.operator IN ('op_bee', 'op_chan', 'op_dew')
              AND MONTH(st.moved_at) IN (1, 3)
        )
)
-- Final projection
SELECT
    id,
    sku_code,
    weight_kg,
    is_active,
    COALESCE(weight_kg, 0.0) AS weight_kg_filled
FROM screened
ORDER BY weight_kg_filled DESC

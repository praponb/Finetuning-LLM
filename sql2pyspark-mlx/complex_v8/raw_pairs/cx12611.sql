WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE qty >= 112
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        CONCAT_WS('-', move_type, operator) AS move_type_operator_label
    FROM recent
    WHERE warehouse_id <= 4
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 299.1
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at
FROM screened
ORDER BY sku_id NULLS LAST, moved_at DESC NULLS FIRST

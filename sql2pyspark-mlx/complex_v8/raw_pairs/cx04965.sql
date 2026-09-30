WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        operator_move_type_label,
        LENGTH(TRIM(move_type)) AS move_type_len,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM valid
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM labeled s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
      AND s.sku_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator_move_type_label,
    move_type_len,
    days_since_moved_at
FROM without_matches
WHERE operator_move_type_label IS NOT NULL
ORDER BY operator_move_type_label DESC, days_since_moved_at

WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE (move_type IN ('TRANSFER', 'OUTBOUND') OR warehouse_id IS NOT NULL)
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.qty > 154
              AND st.sku_id IS NOT NULL
        )
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.move_type IS NOT NULL THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty
    FROM filtered s
)
SELECT
    id,
    move_type,
    moved_at,
    unit_price,
    moved_at_plus_30m,
    move_move_type_count,
    max_move_qty,
    LENGTH(TRIM(move_type)) AS move_type_len
FROM with_counts
ORDER BY max_move_qty

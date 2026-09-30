WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
    WHERE unit_price > 215.4
      AND qty < 314
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 5
              AND st.operator IN ('op_dew', 'op_bee')
        )
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at
    FROM without_matches s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_id,
        move_move_type,
        move_qty,
        move_moved_at,
        FLOOR(move_qty / 100) * 100 AS move_qty_bucket,
        COALESCE(move_qty, 0) AS move_qty_filled
    FROM detail_rows_moves
)
SELECT id, move_id, metric, reading
FROM (
    SELECT id, move_id, move_qty, move_qty_filled
    FROM labeled
) src
UNPIVOT (
    reading FOR metric IN (move_qty, move_qty_filled)
) unp

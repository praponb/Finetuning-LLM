WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label_2
    FROM stock_moves
    WHERE operator <> 'op_bee'
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.moved_at_label,
        st.id AS move_id,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM valid_moves s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at_label,
        move_id,
        move_moved_at,
        DENSE_RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY move_moved_at DESC) AS move_moved_at_dense_rank,
        ROUND(move_qty - AVG(move_qty) OVER w_warehouse_id_move_operator, 2) AS move_qty_vs_warehouse_id_move_operator_avg,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id_move_operator, 0), 4) AS qty_share_of_warehouse_id_move_operator,
        LAG(move_moved_at) OVER w_sku_id_warehouse_id_by_move_2 AS prev_move_moved_at
    FROM detail_rows_moves
    WINDOW w_warehouse_id_move_operator AS (PARTITION BY warehouse_id, move_operator), w_sku_id_warehouse_id_by_move_2 AS (PARTITION BY sku_id, warehouse_id ORDER BY move_moved_at DESC, id, move_id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.qty,
        s.moved_at_label,
        s.move_id,
        s.move_moved_at_dense_rank,
        s.move_qty_vs_warehouse_id_move_operator_avg,
        s.qty_share_of_warehouse_id_move_operator,
        s.prev_move_moved_at,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.move_type
    FROM windowed s
    JOIN stock_moves st ON st.id = s.move_id
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND MONTH(st.moved_at) IN (1, 2)
        )
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id <= 15
        )
)
SELECT
    id,
    moved_at_label,
    move_id,
    move_moved_at_dense_rank,
    qty_share_of_warehouse_id_move_operator,
    prev_move_moved_at,
    move_unit_price,
    move_qty,
    move_type,
    CONCAT(moved_at_label, '#', CAST(move_moved_at_dense_rank AS STRING)) AS moved_at_label_ref,
    ROUND(move_qty / NULLIF(qty, 0), 3) AS move_qty_per_qty
FROM screened
ORDER BY move_unit_price NULLS LAST, id, move_id
LIMIT 10

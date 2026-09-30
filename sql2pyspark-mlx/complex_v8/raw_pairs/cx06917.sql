WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE unit_price < 399.6
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE operator IN ('op_bee', 'op_ann')) AS move_operator_count,
        COUNT(CASE WHEN operator = 'op_eve' THEN 1 END) AS move_operator_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        COALESCE(st.move_operator_count_2, 0) AS move_operator_count_2
    FROM prepared s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_operator_count_2,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty
    FROM with_stats s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM detail_rows_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price <= 330.0
              AND st.moved_at < TIMESTAMP '2025-02-24 09:00:00'
        )
      AND s.sku_id >= 3
)
SELECT
    move_operator_count_2,
    warehouse_id,
    GROUPING_ID() AS gid,
    MIN(move_moved_at) AS first_move_moved_at,
    MIN(moved_at) AS first_moved_at
FROM qualified
GROUP BY GROUPING SETS ((move_operator_count_2, warehouse_id), (move_operator_count_2), (warehouse_id))
ORDER BY move_operator_count_2 NULLS LAST, warehouse_id NULLS LAST

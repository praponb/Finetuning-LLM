WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE operator <> 'op_dew'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM prepared
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.operator_clean,
        st.id AS move_id,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.moved_at AS move_moved_at
    FROM derived s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY sku_id ORDER BY move_moved_at NULLS LAST) AS move_moved_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY move_moved_at NULLS LAST), 3) AS move_moved_at_cume_dist,
        NTILE(3) OVER (PARTITION BY sku_id ORDER BY move_moved_at NULLS LAST, id, move_id) AS move_moved_at_tercile
    FROM line_level_moves
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.operator_clean,
        s.move_id,
        s.move_operator,
        s.move_sku_id,
        s.move_moved_at,
        s.move_moved_at_rank,
        s.move_moved_at_cume_dist,
        st.id AS move_id_2,
        st.moved_at AS move_moved_at_2,
        st.warehouse_id AS move_warehouse_id
    FROM with_trends s
    JOIN stock_moves st ON st.sku_id = s.move_sku_id AND st.warehouse_id <= 2
),
enriched_move AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.operator_clean,
        s.move_id,
        s.move_operator,
        s.move_sku_id,
        s.move_moved_at,
        s.move_moved_at_rank,
        s.move_moved_at_cume_dist,
        s.move_id_2,
        s.move_moved_at_2,
        s.move_warehouse_id,
        st.warehouse_id AS move_warehouse_id_2
    FROM detail_rows_moves s
    LEFT JOIN stock_moves st ON st.id = s.move_id_2
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.operator_clean,
        s.move_id,
        s.move_operator,
        s.move_sku_id,
        s.move_moved_at,
        s.move_moved_at_rank,
        s.move_moved_at_cume_dist,
        s.move_id_2,
        s.move_moved_at_2,
        s.move_warehouse_id,
        s.move_warehouse_id_2,
        w.capacity AS warehouse_capacity
    FROM enriched_move s
    JOIN warehouses w ON w.id = s.move_warehouse_id_2
)
-- Final projection
SELECT
    id,
    warehouse_id,
    operator,
    move_id,
    move_sku_id,
    move_moved_at,
    move_moved_at_rank,
    move_moved_at_cume_dist,
    move_id_2,
    move_moved_at_2,
    move_warehouse_id,
    move_warehouse_id_2,
    warehouse_capacity,
    DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
    YEAR(moved_at) AS moved_at_year
FROM with_details
ORDER BY move_moved_at DESC, move_moved_at_cume_dist

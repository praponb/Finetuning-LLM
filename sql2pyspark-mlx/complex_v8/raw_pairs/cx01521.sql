WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        QUARTER(moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE qty < 372
      AND operator <> 'op_bee'
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at,
        st.warehouse_id AS move_warehouse_id
    FROM valid_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM line_level s
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.move_warehouse_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.move_warehouse_id) AS move_count_2
    FROM with_counts s
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_id,
        s.move_move_type,
        s.move_moved_at,
        s.move_warehouse_id,
        s.last_moved_at,
        st.id AS move_id_2,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at_2,
        st.move_type AS move_move_type_2
    FROM profiled s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
joined_move AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_id,
        s.move_moved_at,
        s.move_warehouse_id,
        s.last_moved_at,
        s.move_id_2,
        s.move_moved_at_2,
        st.moved_at AS move_moved_at_3
    FROM with_children s
    LEFT JOIN stock_moves st ON st.id = s.move_id_2
),
with_details_move AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.move_id,
        s.move_moved_at,
        s.move_warehouse_id,
        s.last_moved_at,
        s.move_id_2,
        s.move_moved_at_2,
        s.move_moved_at_3,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM joined_move s
    JOIN stock_moves st ON st.id = s.move_id
    WHERE st.qty < 275
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    move_id,
    move_moved_at,
    move_warehouse_id,
    last_moved_at,
    move_id_2,
    move_moved_at_2,
    move_moved_at_3,
    move_qty,
    CASE WHEN move_qty >= 355 THEN 'HIGH' WHEN move_qty >= 124 THEN 'MEDIUM' ELSE 'LOW' END AS move_qty_tier,
    FLOOR(move_qty / 100) * 100 AS move_qty_bucket
FROM with_details_move
ORDER BY move_id

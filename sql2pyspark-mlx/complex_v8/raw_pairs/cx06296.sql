WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND qty < 299
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty
    FROM scoped s
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
),
joined_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM with_activity s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM joined_warehouse s
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    moved_at_dayofweek,
    min_move_qty,
    warehouse_wh_code,
    warehouse_opened_on,
    warehouse_city,
    last_moved_at,
    move_count,
    DATE_FORMAT(warehouse_opened_on, 'yyyy-MM-dd') AS warehouse_opened_on_label,
    ROUND((UNIX_TIMESTAMP(last_moved_at) - UNIX_TIMESTAMP(moved_at)) / 3600, 1) AS hours_moved_at_to_last_moved_at
FROM with_counts
WHERE warehouse_wh_code IS NOT NULL
ORDER BY qty

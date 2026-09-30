WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price <= 396.4
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.*,
        sk.sku_code,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM filtered_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price,
        s.warehouse_capacity,
        s.warehouse_wh_code,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.move_type AS move_move_type
    FROM with_details_sku s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.qty < 357
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, measure, amount
FROM (
    SELECT id, move_id, move_qty, warehouse_capacity
    FROM exploded_detail
) src
UNPIVOT (
    amount FOR measure IN (move_qty, warehouse_capacity)
) unp

WITH recent_moves AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM recent_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price > 95.8
              AND st.warehouse_id <= 5
        )
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_len,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM qualified s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    move_type_len,
    warehouse_capacity,
    warehouse_wh_code,
    warehouse_opened_on,
    DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
    DATE_FORMAT(warehouse_opened_on, 'yyyy-MM-dd') AS warehouse_opened_on_label
FROM enriched
WHERE MONTH(moved_at) IN (1, 3)
ORDER BY warehouse_opened_on_label DESC, moved_at_label DESC

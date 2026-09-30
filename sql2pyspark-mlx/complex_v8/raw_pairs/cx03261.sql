WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE operator = 'op_ann'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MAX(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_unit_price
    FROM base s
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    unit_price,
    moved_at_label,
    max_move_unit_price,
    ABS(unit_price - 323.9) AS unit_price_distance
FROM with_counts
ORDER BY id, warehouse_id DESC

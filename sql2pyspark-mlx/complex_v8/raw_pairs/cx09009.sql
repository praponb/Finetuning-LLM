WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        QUARTER(moved_at) AS moved_at_quarter,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE unit_price > 205.1
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.move_type_or_unknown,
        s.moved_at_quarter,
        s.unit_price_plus_qty,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM valid_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.qty < 326
),
warehouse_id_stats AS (
    -- Avg qty per warehouse_id
    SELECT warehouse_id, AVG(qty) AS avg_qty
    FROM detail_rows_moves
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM detail_rows_moves s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.qty > g.avg_qty
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    unit_price,
    moved_at_quarter,
    unit_price_plus_qty,
    move_unit_price,
    move_qty,
    move_moved_at,
    avg_qty,
    qty_minus_avg
FROM above_average
WHERE move_type_or_unknown IN ('OUTBOUND', 'UNKNOWN')
ORDER BY unit_price, moved_at_quarter

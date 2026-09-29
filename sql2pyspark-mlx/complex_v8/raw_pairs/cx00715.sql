WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 14
),
move_type_stats AS (
    -- Max qty per move_type
    SELECT move_type, MAX(qty) AS max_qty
    FROM recent_moves
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_qty, ROUND(s.qty - g.max_qty, 2) AS qty_minus_max
    FROM recent_moves s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty = g.max_qty
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.max_qty,
        s.qty_minus_max,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.sku_id AS move_sku_id,
        st.move_type AS move_move_type
    FROM outliers s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric_name, metric_value
FROM (
    SELECT id, move_id, qty_minus_max, max_qty
    FROM line_level
) src
UNPIVOT (
    metric_value FOR metric_name IN (qty_minus_max, max_qty)
) unp

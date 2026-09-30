WITH recent_moves AS (
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
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price >= 200.7
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM recent_moves s
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.moved_at AS move_moved_at
    FROM with_stats s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND HOUR(st.moved_at) BETWEEN 10 AND 13
)
SELECT *
FROM (
    SELECT sku_id, move_sku_id, operator, qty
    FROM line_level
) src
PIVOT (
    AVG(qty) AS avg_v, SUM(qty) AS sum_v
    FOR operator IN ('op_bee', 'op_chan')
)

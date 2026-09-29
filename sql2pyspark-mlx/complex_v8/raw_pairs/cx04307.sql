WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        HOUR(moved_at) AS moved_at_hour,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND warehouse_id <= 3
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty
    FROM valid s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, operator, qty_bucket
    FROM with_stats
) src
PIVOT (
    COUNT(qty_bucket)
    FOR operator IN ('op_ann', 'op_bee', 'op_eve')
)
ORDER BY warehouse_id

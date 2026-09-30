WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_unit_price
    FROM base_moves s
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    last_moved_at,
    move_count,
    min_move_unit_price,
    LENGTH(TRIM(operator)) AS operator_len,
    LENGTH(TRIM(move_type)) AS move_type_len
FROM with_stats
ORDER BY last_moved_at, min_move_unit_price NULLS LAST

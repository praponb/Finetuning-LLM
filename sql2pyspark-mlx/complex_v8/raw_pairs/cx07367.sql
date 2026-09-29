WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price < 313.8
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(CASE WHEN move_type = 'INBOUND' THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM filtered s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT *
FROM (
    SELECT sku_id, warehouse_id, operator, qty
    FROM summarized_rows
) src
PIVOT (
    AVG(qty)
    FOR operator IN ('op_chan', 'op_dew')
)
ORDER BY sku_id, warehouse_id

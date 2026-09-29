WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count_2
    FROM base_moves s
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, move_count_2, operator, unit_price
    FROM without_matches
) src
PIVOT (
    SUM(unit_price) AS amount, COUNT(unit_price) AS n
    FOR operator IN ('op_eve', 'op_chan', 'op_bee')
)
ORDER BY sku_id, move_count_2

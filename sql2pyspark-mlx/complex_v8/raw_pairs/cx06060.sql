WITH recent AS (
    -- Filter stock_moves
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
    WHERE move_type NOT IN ('TRANSFER', 'ADJUST')
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE operator = 'op_dew') AS move_operator_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM recent s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator <> 'op_dew'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at, move_type, operator, move_operator_count
    FROM screened
) src
PIVOT (
    MIN(move_operator_count)
    FOR operator IN ('op_chan', 'op_dew')
)

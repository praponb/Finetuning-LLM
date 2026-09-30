WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ABS(unit_price - 313.8) AS unit_price_distance
    FROM stock_moves
    WHERE operator = 'op_chan'
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM prepared_moves s
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.moved_at >= TIMESTAMP '2025-01-31 08:00:00'
              AND st.unit_price < 363.6
        )
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    warehouse_id,
    move_count,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE move_type = 'OUTBOUND') AS n_move_type_outbound,
    ROUND(STDDEV(unit_price_distance), 2) AS stddev_unit_price_distance,
    COUNT(DISTINCT operator) AS distinct_operators
FROM without_matches
GROUP BY ROLLUP(move_type, warehouse_id, move_count)
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST, move_count NULLS LAST

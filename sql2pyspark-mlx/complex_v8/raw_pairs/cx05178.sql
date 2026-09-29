WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty >= 199
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.unit_price BETWEEN 112.8 AND 341.5
        )
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM screened s
),
summary AS (
    -- Summarise per group
    SELECT
        warehouse_id,
        ROUND(AVG(total_move_qty), 2) AS avg_total_move_qty,
        SUM(CASE WHEN operator = 'op_dew' THEN days_since_moved_at ELSE 0 END) AS days_since_moved_at_operator_op_dew
    FROM with_stats
    GROUP BY warehouse_id
)
SELECT
    warehouse_id,
    avg_total_move_qty,
    days_since_moved_at_operator_op_dew,
    CASE WHEN avg_total_move_qty >= 2100.3 THEN 'HIGH' WHEN avg_total_move_qty >= 1050.7 THEN 'MEDIUM' ELSE 'LOW' END AS avg_total_move_qty_tier,
    ROUND(avg_total_move_qty * 0.25, 2) AS avg_total_move_qty_adj
FROM summary
WHERE days_since_moved_at_operator_op_dew < 2975
ORDER BY days_since_moved_at_operator_op_dew, warehouse_id

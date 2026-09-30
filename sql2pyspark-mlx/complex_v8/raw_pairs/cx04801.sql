WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE unit_price BETWEEN 164.1 AND 330.0
      AND operator IN ('op_dew', 'op_ann')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price_filled,
        LAST_VALUE(moved_at) OVER (PARTITION BY move_type, operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        ROUND(qty - AVG(qty) OVER w_warehouse_id_sku_id, 2) AS qty_vs_warehouse_id_sku_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, operator ORDER BY qty DESC, id), 3) AS qty_cume_dist,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id_sku_id, 0), 4) AS qty_share_of_warehouse_id_sku_id
    FROM scoped
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty_2,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM metrics s
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'TRANSFER'
              AND st.moved_at >= TIMESTAMP '2025-02-12 10:00:00'
        )
      AND s.qty_share_of_warehouse_id_sku_id BETWEEN 0.1 AND 0.7
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(operator, 'ALL') AS operator,
    COALESCE(move_type, 'ALL') AS move_type,
    GROUPING(operator) AS is_total_operator,
    MIN(first_moved_at) AS first_first_moved_at
FROM screened
GROUP BY GROUPING SETS ((operator, move_type), (operator), (move_type), ())
ORDER BY operator NULLS LAST, move_type NULLS LAST

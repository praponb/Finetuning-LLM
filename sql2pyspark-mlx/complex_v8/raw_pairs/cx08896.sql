WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
    FROM stock_moves
    WHERE qty < 349
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        ROUND(qty - AVG(qty) OVER (PARTITION BY MONTH(moved_at_plus_15m)), 2) AS qty_vs_moved_at_plus_15m_month_avg,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY operator ORDER BY qty ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        DENSE_RANK() OVER w_operator_by_qty_ties AS qty_dense_rank
    FROM scoped
    WINDOW w_operator_by_qty_ties AS (PARTITION BY operator ORDER BY qty ASC NULLS LAST)
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        qty_vs_moved_at_plus_15m_month_avg,
        first_known_qty,
        qty_dense_rank,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY qty_dense_rank, warehouse_id)) AS distinct_move_type_in_qty_dense_rank_warehouse_i,
        NTILE(5) OVER (ORDER BY qty_vs_moved_at_plus_15m_month_avg DESC, id) AS qty_vs_moved_at_plus_15m_month_avg_quintile
    FROM screened
)
SELECT
    distinct_move_type_in_qty_dense_rank_warehouse_i,
    warehouse_id,
    qty_vs_moved_at_plus_15m_month_avg_quintile,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
    ROUND(STDDEV(first_known_qty), 2) AS stddev_first_known_qty
FROM windowed
GROUP BY ROLLUP(distinct_move_type_in_qty_dense_rank_warehouse_i, warehouse_id, qty_vs_moved_at_plus_15m_month_avg_quintile)
ORDER BY distinct_move_type_in_qty_dense_rank_warehouse_i NULLS LAST, warehouse_id NULLS LAST, qty_vs_moved_at_plus_15m_month_avg_quintile NULLS LAST

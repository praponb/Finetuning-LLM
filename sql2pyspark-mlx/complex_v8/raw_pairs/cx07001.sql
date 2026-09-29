WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY move_type, operator ORDER BY qty, id) AS qty_tercile,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_type), 0), 4) AS qty_share_of_move_type,
        DENSE_RANK() OVER (PARTITION BY operator, warehouse_id ORDER BY qty NULLS LAST, id) AS qty_dense_rank,
        NTH_VALUE(qty, 2) OVER (PARTITION BY move_type ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LEAD(qty, 2) OVER (PARTITION BY move_type ORDER BY qty DESC, id) AS next2_qty,
        MAX(qty) OVER (PARTITION BY operator, warehouse_id ORDER BY qty NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_max_4,
        ROUND(AVG(qty) OVER (ORDER BY qty NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4,
        ROW_NUMBER() OVER (ORDER BY qty ASC NULLS LAST, id) AS seq_no,
        LEAD(qty) OVER (PARTITION BY operator, warehouse_id ORDER BY qty NULLS LAST, id) AS next_qty,
        ROUND(AVG(qty) OVER (PARTITION BY move_type, operator ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        DENSE_RANK() OVER w_warehouse_id_by_qty_ties AS qty_dense_rank_2,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type), 2) AS qty_vs_move_type_avg,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id
    FROM prepared
    WINDOW w_warehouse_id_by_qty_ties AS (PARTITION BY warehouse_id ORDER BY qty ASC NULLS LAST)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE qty_dense_rank <= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_qty_dense_rank_2_ties AS qty_dense_rank_2_rank,
        MAX(qty_vs_move_type_avg) OVER (PARTITION BY move_type) AS move_type_max_qty_vs_move_type_avg,
        ROW_NUMBER() OVER (ORDER BY qty_dense_rank_2 NULLS LAST, id) AS rn,
        NTILE(3) OVER (ORDER BY qty_avg_4 NULLS LAST, id) AS qty_avg_4_tercile
    FROM first_rows
    WINDOW w_all_by_qty_dense_rank_2_ties AS (ORDER BY qty_dense_rank_2 NULLS LAST)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    qty_tercile,
    qty_share_of_move_type,
    qty_dense_rank,
    second_qty,
    next2_qty,
    qty_max_4,
    qty_avg_4,
    seq_no,
    qty_dense_rank_2,
    qty_vs_move_type_avg,
    rows_in_warehouse_id_sku_id,
    rn,
    qty_avg_4_tercile,
    ROUND(qty_tercile / NULLIF(qty_avg_3, 0), 3) AS qty_tercile_per_qty_avg_3,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM windowed
ORDER BY qty_tercile DESC, operator DESC NULLS FIRST

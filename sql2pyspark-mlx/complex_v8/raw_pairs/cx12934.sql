WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE operator <> 'op_ann'
      AND sku_id >= 3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id))) / 60, 1) AS minutes_since_prev_moved_at,
        MIN(qty) OVER (PARTITION BY operator) AS operator_min_qty,
        ROUND(PERCENT_RANK() OVER w_sku_id_warehouse_id_by_moved, 3) AS moved_at_percent_rank,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        LAG(qty, 1, 0) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_qty,
        MIN(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        DENSE_RANK() OVER w_move_type_by_moved_at_ties AS moved_at_dense_rank,
        COUNT(*) OVER w_operator AS rows_in_operator,
        SUM(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator,
        ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at ASC, id) AS seq_no,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank_2
    FROM prepared_moves
    WINDOW w_sku_id_warehouse_id_by_moved AS (PARTITION BY sku_id, warehouse_id ORDER BY moved_at ASC), w_move_type_by_moved_at_ties AS (PARTITION BY move_type ORDER BY moved_at), w_operator AS (PARTITION BY operator), w_sku_id_warehouse_id_by_moved_2 AS (PARTITION BY sku_id, warehouse_id ORDER BY moved_at ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(moved_at_dense_rank, 3) OVER (ORDER BY minutes_since_prev_moved_at, id) AS next3_moved_at_dense_rank,
        ROUND(moved_at_dense_rank_2 / NULLIF(SUM(moved_at_dense_rank_2) OVER (PARTITION BY distinct_sku_id_in_operator), 0), 4) AS moved_at_dense_rank_2_share_of_distinct_sku_id_i,
        FIRST_VALUE(prev_qty) IGNORE NULLS OVER (ORDER BY minutes_since_prev_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_qty,
        ROW_NUMBER() OVER (ORDER BY minutes_since_prev_moved_at, id) AS minutes_since_prev_moved_at_seq,
        prev_qty - LAG(prev_qty) OVER w_distinct_sku_id_in_operator_2 AS prev_qty_change,
        SUM(moved_at_dense_rank_2) OVER (PARTITION BY distinct_sku_id_in_operator, warehouse_id ORDER BY min_qty_so_far NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS moved_at_dense_rank_2_sum_2_prior,
        ROUND(AVG(running_qty) OVER (PARTITION BY distinct_sku_id_in_operator), 2) AS distinct_sku_id_in_operator_avg_running_qty,
        ROUND(AVG(min_qty_so_far) OVER w_distinct_sku_id_in_operator, 2) AS distinct_sku_id_in_operator_avg_min_qty_so_far
    FROM with_windows
    WINDOW w_distinct_sku_id_in_operator_2 AS (PARTITION BY distinct_sku_id_in_operator, warehouse_id ORDER BY min_qty_so_far NULLS LAST, id), w_distinct_sku_id_in_operator AS (PARTITION BY distinct_sku_id_in_operator)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    minutes_since_prev_moved_at,
    operator_min_qty,
    min_qty_so_far,
    moved_at_dense_rank,
    rows_in_operator,
    seq_no,
    qty_vs_operator_avg,
    moved_at_dense_rank_2,
    next3_moved_at_dense_rank,
    moved_at_dense_rank_2_share_of_distinct_sku_id_i,
    first_known_prev_qty,
    minutes_since_prev_moved_at_seq,
    moved_at_dense_rank_2_sum_2_prior,
    distinct_sku_id_in_operator_avg_min_qty_so_far
FROM metrics
ORDER BY moved_at_dense_rank_2_share_of_distinct_sku_id_i DESC, rows_in_operator

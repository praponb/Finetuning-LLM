WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE operator NOT IN ('op_bee', 'op_chan')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_operator) AS distinct_warehouse_id_in_sku_id_operator,
        NTILE(10) OVER w_sku_id_by_moved_at AS moved_at_decile,
        RANK() OVER (PARTITION BY sku_id, operator ORDER BY moved_at, id) AS moved_at_rank,
        MIN(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        SUM(qty) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_sum_qty,
        ROW_NUMBER() OVER (ORDER BY qty DESC, id) AS rn,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id, operator), 0), 4) AS qty_share_of_sku_id_operator,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY sku_id, operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        MIN(qty) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_qty,
        DENSE_RANK() OVER (ORDER BY qty DESC) AS qty_dense_rank
    FROM base
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator), w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at ASC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        first_known_moved_at,
        distinct_warehouse_id_in_sku_id_operator,
        moved_at_decile,
        min_qty_so_far,
        sku_id_operator_sum_qty,
        rn,
        qty_share_of_sku_id_operator,
        last_known_qty,
        sku_id_operator_min_qty,
        qty_dense_rank
    FROM windowed
    WHERE moved_at_rank <= 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        distinct_warehouse_id_in_sku_id_operator,
        min_qty_so_far,
        sku_id_operator_sum_qty,
        rn,
        qty_share_of_sku_id_operator,
        last_known_qty,
        qty_dense_rank,
        LAG(moved_at) OVER w_all_by_distinct_warehouse_id AS prev_moved_at,
        DENSE_RANK() OVER w_rn_by_qty_dense_rank_ties AS qty_dense_rank_dense_rank,
        ROW_NUMBER() OVER (ORDER BY distinct_warehouse_id_in_sku_id_operator DESC, id) AS distinct_warehouse_id_in_sku_id_operator_seq,
        NTH_VALUE(qty_share_of_sku_id_operator, 2) OVER (ORDER BY distinct_warehouse_id_in_sku_id_operator DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_share_of_sku_id_operator,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id
    FROM top_rows
    WINDOW w_all_by_distinct_warehouse_id AS (ORDER BY distinct_warehouse_id_in_sku_id_operator DESC, id), w_rn_by_qty_dense_rank_ties AS (PARTITION BY rn ORDER BY qty_dense_rank)
)
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    distinct_warehouse_id_in_sku_id_operator,
    sku_id_operator_sum_qty,
    rn,
    qty_share_of_sku_id_operator,
    qty_dense_rank,
    prev_moved_at,
    distinct_warehouse_id_in_sku_id_operator_seq,
    second_qty_share_of_sku_id_operator,
    rows_in_sku_id,
    DATE_TRUNC('QUARTER', prev_moved_at) AS prev_moved_at_quarter,
    DATEDIFF(DATE '2026-01-01', prev_moved_at) AS days_since_prev_moved_at
FROM with_windows
ORDER BY distinct_warehouse_id_in_sku_id_operator_seq DESC, rows_in_sku_id NULLS LAST

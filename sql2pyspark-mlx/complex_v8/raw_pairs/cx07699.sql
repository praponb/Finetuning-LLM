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
        unit_price
    FROM stock_moves
    WHERE (operator = 'op_dew' OR unit_price >= 86.3)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY TO_DATE(moved_at), operator), 0), 4) AS qty_share_of_moved_at_to_date_operator,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY TO_DATE(moved_at), operator)) AS distinct_move_type_in_moved_at_to_date_operator,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY TO_DATE(moved_at), operator)) AS distinct_sku_id_in_moved_at_to_date_operator,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty), 3) AS qty_percent_rank,
        FIRST_VALUE(unit_price) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY TO_DATE(moved_at), operator)) AS distinct_warehouse_id_in_moved_at_to_date_operat,
        DENSE_RANK() OVER w_all_by_unit_price_ties AS unit_price_dense_rank,
        COUNT(*) OVER w_moved_at_to_date_operator AS rows_in_moved_at_to_date_operator,
        MAX(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        MIN(qty) OVER (PARTITION BY TO_DATE(moved_at), operator) AS moved_at_to_date_operator_min_qty,
        MIN(unit_price) OVER (PARTITION BY TO_DATE(moved_at), operator) AS moved_at_to_date_operator_min_unit_price,
        LAST_VALUE(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM base
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price DESC), w_moved_at_to_date_operator AS (PARTITION BY TO_DATE(moved_at), operator)
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(first_unit_price) OVER (PARTITION BY distinct_sku_id_in_moved_at_to_date_operator ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_unit_price,
        LAG(first_unit_price, 1, 0) OVER (PARTITION BY unit_price_dense_rank ORDER BY unit_price_dense_rank, id) AS prev_first_unit_price,
        NTILE(4) OVER (PARTITION BY distinct_sku_id_in_moved_at_to_date_operator ORDER BY moved_at DESC, id) AS moved_at_quartile,
        SUM(last_unit_price) OVER w_unit_price_dense_rank_distin AS unit_price_dense_rank_distinct_sku_id_in_moved_a,
        ROW_NUMBER() OVER w_distinct_sku_id_in_moved_at_2 AS seq_no
    FROM windowed
    WINDOW w_unit_price_dense_rank_distin AS (PARTITION BY unit_price_dense_rank, distinct_sku_id_in_moved_at_to_date_operator), w_distinct_sku_id_in_moved_at_2 AS (PARTITION BY distinct_sku_id_in_moved_at_to_date_operator ORDER BY moved_at DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        CASE WHEN qty >= 372 THEN 'large' WHEN qty >= 199 THEN 'medium' ELSE 'small' END AS qty_tier,
        COUNT(*) FILTER (WHERE operator IN ('op_chan', 'op_bee', 'op_eve')) AS n_operator_in_op_chan_op_bee_o,
        COUNT(*) AS n_rows,
        SUM(moved_at_to_date_operator_min_unit_price) FILTER (WHERE move_type IN ('INBOUND', 'TRANSFER')) AS moved_at_to_date_operator_min_unit_price_move_ty
    FROM windowed_2
    GROUP BY CASE WHEN qty >= 372 THEN 'large' WHEN qty >= 199 THEN 'medium' ELSE 'small' END
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_n_rows_n_operator_in_op_chan AS rows_in_n_rows_n_operator_in_op_chan_op_bee_o,
        FIRST_VALUE(n_rows) IGNORE NULLS OVER (PARTITION BY qty_tier ORDER BY moved_at_to_date_operator_min_unit_price_move_ty DESC, qty_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_rows
    FROM totals
    WINDOW w_n_rows_n_operator_in_op_chan AS (PARTITION BY n_rows, n_operator_in_op_chan_op_bee_o)
)
SELECT
    qty_tier,
    n_operator_in_op_chan_op_bee_o,
    n_rows,
    moved_at_to_date_operator_min_unit_price_move_ty,
    first_known_n_rows
FROM with_windows
ORDER BY n_rows, first_known_n_rows

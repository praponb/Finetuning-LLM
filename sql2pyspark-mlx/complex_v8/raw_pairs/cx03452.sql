WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        LENGTH(TRIM(move_type)) AS move_type_len,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE operator = 'op_ann'
      AND moved_at >= TIMESTAMP '2025-01-25 10:00:00'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(qty_per_unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY move_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty_per_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY moved_at), 3) AS moved_at_percent_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type_len, TO_DATE(moved_at))) AS distinct_sku_id_in_move_type_len_moved_at_to_dat,
        RANK() OVER (PARTITION BY move_type_len, TO_DATE(moved_at) ORDER BY move_type_len) AS move_type_len_rank,
        RANK() OVER (PARTITION BY sku_id ORDER BY move_type_len, id) AS move_type_len_rank_2,
        COUNT(*) OVER (PARTITION BY move_type_len) AS rows_in_move_type_len,
        RANK() OVER (PARTITION BY operator ORDER BY moved_at) AS moved_at_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type_len) AS distinct_sku_id_in_move_type_len,
        RANK() OVER w_sku_id_by_move_type_len_ties AS move_type_len_rank_3,
        RANK() OVER (PARTITION BY operator ORDER BY moved_at, id) AS moved_at_rank_2,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type_len)) AS distinct_warehouse_id_in_move_type_len,
        MAX(qty_per_unit_price) OVER (PARTITION BY move_type_len) AS move_type_len_max_qty_per_unit_price,
        FIRST_VALUE(moved_at) OVER (PARTITION BY move_type_len, TO_DATE(moved_at) ORDER BY move_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        SUM(qty) OVER (PARTITION BY move_type_len, TO_DATE(moved_at) ORDER BY move_type_len, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_sum_2
    FROM filtered_moves
    WINDOW w_move_type_len AS (PARTITION BY move_type_len), w_sku_id_by_move_type_len_ties AS (PARTITION BY sku_id ORDER BY move_type_len)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(operator)) AS operator_len,
        ROUND((UNIX_TIMESTAMP(first_moved_at) - UNIX_TIMESTAMP(moved_at)) / 60, 1) AS minutes_moved_at_to_first_moved_at
    FROM ranked
    WHERE rows_in_move_type_len <= 11
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        qty,
        move_type_len,
        qty_per_unit_price,
        last_qty_per_unit_price,
        moved_at_percent_rank,
        distinct_sku_id_in_move_type_len_moved_at_to_dat,
        move_type_len_rank,
        move_type_len_rank_2,
        rows_in_move_type_len,
        moved_at_rank,
        move_type_len_rank_3,
        distinct_warehouse_id_in_move_type_len,
        move_type_len_max_qty_per_unit_price,
        qty_sum_2,
        operator_len,
        minutes_moved_at_to_first_moved_at,
        LAG(move_type_len, 2) OVER (PARTITION BY operator, operator_len ORDER BY move_type_len, id) AS prev2_move_type_len,
        ROUND(moved_at_rank_2 - AVG(moved_at_rank_2) OVER w_distinct_sku_id_in_move_type, 2) AS moved_at_rank_2_vs_distinct_sku_id_in_move_type_,
        NTH_VALUE(move_type_len_rank_2, 2) OVER (PARTITION BY moved_at_rank, distinct_sku_id_in_move_type_len_moved_at_to_dat ORDER BY move_type_len_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_move_type_len_rank_2
    FROM derived
    WINDOW w_distinct_sku_id_in_move_type AS (PARTITION BY distinct_sku_id_in_move_type_len, operator)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    move_type_len,
    qty_per_unit_price,
    last_qty_per_unit_price,
    moved_at_percent_rank,
    distinct_sku_id_in_move_type_len_moved_at_to_dat,
    move_type_len_rank,
    move_type_len_rank_2,
    rows_in_move_type_len,
    moved_at_rank,
    move_type_len_rank_3,
    distinct_warehouse_id_in_move_type_len,
    move_type_len_max_qty_per_unit_price,
    qty_sum_2,
    operator_len,
    minutes_moved_at_to_first_moved_at,
    prev2_move_type_len,
    moved_at_rank_2_vs_distinct_sku_id_in_move_type_,
    second_move_type_len_rank_2,
    GREATEST(move_type_len, rows_in_move_type_len) AS greatest_move_type_len_rows_in_move_type_len,
    move_type_len_rank_2 + moved_at_percent_rank AS move_type_len_rank_2_plus_moved_at_percent_rank
FROM ranked_2
ORDER BY second_move_type_len_rank_2 NULLS LAST, id
LIMIT 5

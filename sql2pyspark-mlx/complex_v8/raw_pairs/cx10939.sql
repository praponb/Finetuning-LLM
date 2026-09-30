WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        CASE WHEN move_type IN ('INBOUND', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE unit_price BETWEEN 136.0 AND 307.8
      AND move_type NOT IN ('INBOUND', 'ADJUST')
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_group,
        least_qty_unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM valid
),
summary AS (
    -- Summarise per group
    SELECT
        move_type,
        move_type_len,
        warehouse_id,
        SUM(least_qty_unit_price) AS total_least_qty_unit_price,
        COUNT(*) FILTER (WHERE operator IN ('op_bee', 'op_dew', 'op_chan')) AS n_operator_in_op_bee_op_dew_op,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM enriched
    GROUP BY move_type, move_type_len, warehouse_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        move_type,
        move_type_len,
        warehouse_id,
        total_least_qty_unit_price,
        operator_list,
        median_qty,
        DENSE_RANK() OVER w_all_by_total_least_qty_unit AS total_least_qty_unit_price_dense_rank,
        ROUND(AVG(median_qty) OVER (PARTITION BY move_type_len), 2) AS move_type_len_avg_median_qty,
        LAG(move_type_len) OVER w_n_operator_in_op_bee_op_dew AS prev_move_type_len,
        total_least_qty_unit_price - LAG(total_least_qty_unit_price) OVER (PARTITION BY n_operator_in_op_bee_op_dew_op ORDER BY total_least_qty_unit_price, move_type, move_type_len, warehouse_id) AS total_least_qty_unit_price_change,
        NTILE(4) OVER (PARTITION BY n_operator_in_op_bee_op_dew_op ORDER BY total_least_qty_unit_price, move_type, move_type_len, warehouse_id) AS total_least_qty_unit_price_quartile,
        SUM(move_type_len) OVER (PARTITION BY move_type ORDER BY n_operator_in_op_bee_op_dew_op, move_type, move_type_len, warehouse_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS move_type_len_sum_3
    FROM summary
    WINDOW w_all_by_total_least_qty_unit AS (ORDER BY total_least_qty_unit_price), w_n_operator_in_op_bee_op_dew AS (PARTITION BY n_operator_in_op_bee_op_dew_op ORDER BY total_least_qty_unit_price, move_type, move_type_len, warehouse_id)
)
-- Final projection
SELECT
    move_type,
    move_type_len,
    total_least_qty_unit_price,
    operator_list,
    median_qty,
    total_least_qty_unit_price_dense_rank,
    move_type_len_avg_median_qty,
    prev_move_type_len,
    total_least_qty_unit_price_change,
    total_least_qty_unit_price_quartile,
    move_type_len_sum_3
FROM with_windows
WHERE move_type_len_sum_3 <= 36
ORDER BY prev_move_type_len DESC, median_qty

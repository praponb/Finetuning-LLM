WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
      AND move_type NOT IN ('OUTBOUND', 'TRANSFER')
),
grouped AS (
    -- Summarise per group
    SELECT
        operator,
        moved_at_hour,
        operator_len,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE operator IN ('op_bee', 'op_dew')) AS n_operator_in_op_bee_op_dew,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        ROUND(SUM(moved_at_hour) / NULLIF(SUM(unit_price), 0), 3) AS moved_at_hour_to_unit_price_ratio,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
    FROM base_moves
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        operator,
        moved_at_hour,
        operator_len,
        n_operator_in_op_bee_op_dew,
        distinct_move_types,
        distinct_warehouse_ids,
        MAX(n_operator_in_op_bee_op_dew) OVER (PARTITION BY moved_at_hour ORDER BY moved_at_hour_to_unit_price_ratio, operator, moved_at_hour, operator_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_operator_in_op_bee_op_dew_so_far,
        DENSE_RANK() OVER w_moved_at_hour_by_moved_at_ho_2 AS moved_at_hour_to_unit_price_ratio_dense_rank,
        FIRST_VALUE(operator_len) IGNORE NULLS OVER (PARTITION BY moved_at_hour ORDER BY moved_at_hour_to_unit_price_ratio, operator, moved_at_hour, operator_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_operator_len,
        SUM(n_operator_in_op_bee_op_dew) OVER (ORDER BY moved_at_hour_to_unit_price_ratio DESC, operator, moved_at_hour, operator_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_operator_in_op_bee_op_dew,
        ROUND(distinct_move_types / NULLIF(SUM(distinct_move_types) OVER w_distinct_warehouse_ids, 0), 4) AS distinct_move_types_share_of_distinct_warehouse_
    FROM grouped
    WINDOW w_moved_at_hour_by_moved_at_ho_2 AS (PARTITION BY moved_at_hour ORDER BY moved_at_hour_to_unit_price_ratio), w_distinct_warehouse_ids AS (PARTITION BY distinct_warehouse_ids)
)
-- Subtotals with CUBE
SELECT
    operator_len,
    distinct_move_types,
    moved_at_hour,
    SUM(distinct_move_types) AS total_distinct_move_types,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM with_windows
GROUP BY CUBE(operator_len, distinct_move_types, moved_at_hour)
ORDER BY operator_len NULLS LAST, distinct_move_types NULLS LAST, moved_at_hour NULLS LAST

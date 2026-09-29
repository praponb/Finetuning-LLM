WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        COALESCE(qty, 0) AS qty_filled,
        UPPER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 08:00:00'
      AND qty > 98
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, move_type), 0), 4) AS qty_share_of_operator_move_type,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_sku_id_by_moved_at)) / 60, 1) AS minutes_since_prev_moved_at,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY qty NULLS LAST), 3) AS qty_percent_rank,
        FIRST_VALUE(qty_filled) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_filled,
        ROW_NUMBER() OVER w_warehouse_id_by_moved_at AS moved_at_seq,
        SIZE(COLLECT_SET(move_type) OVER w_sku_id_operator) AS distinct_move_type_in_sku_id_operator,
        MAX(qty) OVER (PARTITION BY operator, move_type) AS operator_move_type_max_qty,
        ROUND(qty_filled / NULLIF(SUM(qty_filled) OVER (PARTITION BY operator, move_type), 0), 4) AS qty_filled_share_of_operator_move_type
    FROM filtered
    WINDOW w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at DESC, id), w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at, id), w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        qty_filled,
        move_type_clean,
        qty_share_of_operator_move_type,
        second_moved_at,
        minutes_since_prev_moved_at,
        last_known_qty,
        qty_percent_rank,
        first_qty_filled,
        distinct_move_type_in_sku_id_operator,
        operator_move_type_max_qty,
        qty_filled_share_of_operator_move_type
    FROM windowed
    WHERE moved_at_seq = 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        distinct_move_type_in_sku_id_operator,
        DATE_TRUNC('MONTH', second_moved_at) AS second_moved_at_month,
        operator,
        SUM(qty_share_of_operator_move_type) AS total_qty_share_of_operator_move_type,
        MIN(distinct_move_type_in_sku_id_operator) AS min_distinct_move_type_in_sku_id_operator,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ROUND(STDDEV(qty), 2) AS stddev_qty,
        SUM(last_known_qty) FILTER (WHERE operator NOT IN ('op_ann', 'op_dew')) AS last_known_qty_operator_not_in_op_ann_op_de,
        COUNT(*) AS row_count
    FROM leaders
    WHERE operator = 'op_bee'
    GROUP BY distinct_move_type_in_sku_id_operator, DATE_TRUNC('MONTH', second_moved_at), operator
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        distinct_move_type_in_sku_id_operator,
        second_moved_at_month,
        operator,
        total_qty_share_of_operator_move_type,
        min_distinct_move_type_in_sku_id_operator,
        stddev_qty,
        last_known_qty_operator_not_in_op_ann_op_de,
        DENSE_RANK() OVER w_distinct_move_type_in_sku_id AS last_known_qty_operator_not_in_op_ann_op_de_dens,
        ROUND(AVG(row_count) OVER (PARTITION BY operator ORDER BY min_distinct_move_type_in_sku_id_operator, distinct_move_type_in_sku_id_operator, second_moved_at_month, operator ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_4,
        NTH_VALUE(row_count, 2) OVER (PARTITION BY operator ORDER BY min_distinct_move_type_in_sku_id_operator, distinct_move_type_in_sku_id_operator, second_moved_at_month, operator ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY row_count, distinct_move_type_in_sku_id_operator)) AS distinct_operator_in_row_count_distinct_move_typ
    FROM rollup_base
    WINDOW w_distinct_move_type_in_sku_id AS (PARTITION BY distinct_move_type_in_sku_id_operator, min_distinct_move_type_in_sku_id_operator ORDER BY last_known_qty_operator_not_in_op_ann_op_de ASC)
)
-- Final projection
SELECT
    distinct_move_type_in_sku_id_operator,
    operator,
    total_qty_share_of_operator_move_type,
    min_distinct_move_type_in_sku_id_operator,
    stddev_qty,
    last_known_qty_operator_not_in_op_ann_op_de,
    row_count_avg_4,
    distinct_operator_in_row_count_distinct_move_typ,
    row_count_avg_4 + stddev_qty AS row_count_avg_4_plus_stddev_qty
FROM with_trends
ORDER BY row_count_avg_4_plus_stddev_qty DESC, distinct_operator_in_row_count_distinct_move_typ

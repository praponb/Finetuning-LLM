WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE unit_price > 125.9
      AND warehouse_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        qty,
        operator,
        DENSE_RANK() OVER w_warehouse_id_operator_by_uni AS unit_price_bucket_dense_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER w_operator_move_type) AS distinct_warehouse_id_in_operator_move_type,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type), 2) AS qty_vs_move_type_avg,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, move_type), 2) AS qty_vs_operator_move_type_avg,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type) AS distinct_sku_id_in_move_type,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at_date ASC), 3) AS moved_at_date_cume_dist,
        MIN(qty) OVER (PARTITION BY operator, move_type) AS operator_move_type_min_qty,
        FIRST_VALUE(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROW_NUMBER() OVER w_warehouse_id_move_type_by_mo_2 AS moved_at_date_seq,
        LAG(qty, 1, 0) OVER (PARTITION BY operator ORDER BY unit_price_bucket DESC, id) AS prev_qty
    FROM base_moves
    WINDOW w_warehouse_id_operator_by_uni AS (PARTITION BY warehouse_id, operator ORDER BY unit_price_bucket), w_operator_move_type AS (PARTITION BY operator, move_type), w_move_type AS (PARTITION BY move_type), w_warehouse_id_move_type_by_mo_2 AS (PARTITION BY warehouse_id, move_type ORDER BY moved_at_date ASC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        qty,
        operator,
        distinct_warehouse_id_in_operator_move_type,
        qty_vs_operator_move_type_avg,
        distinct_sku_id_in_move_type,
        moved_at_date_cume_dist,
        first_qty,
        moved_at_date_seq,
        ROUND(AVG(first_qty) OVER (PARTITION BY moved_at_date_seq, move_type), 2) AS moved_at_date_seq_move_type_avg_first_qty,
        ROUND(AVG(qty_vs_move_type_avg) OVER (ORDER BY first_qty ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_vs_move_type_avg_avg_6_prior,
        ROUND(AVG(operator_move_type_min_qty) OVER (PARTITION BY moved_at_date_seq, move_type), 2) AS moved_at_date_seq_move_type_avg_operator_move_ty,
        NTILE(3) OVER (PARTITION BY distinct_sku_id_in_move_type, moved_at_date_seq ORDER BY prev_qty ASC, id) AS prev_qty_tercile
    FROM sequenced
)
-- Final projection
SELECT
    id,
    qty,
    operator,
    qty_vs_operator_move_type_avg,
    distinct_sku_id_in_move_type,
    moved_at_date_cume_dist,
    moved_at_date_seq,
    moved_at_date_seq_move_type_avg_first_qty,
    qty_vs_move_type_avg_avg_6_prior,
    moved_at_date_seq_move_type_avg_operator_move_ty,
    prev_qty_tercile,
    SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
    CONCAT(move_type, '#', CAST(moved_at_date_seq AS STRING)) AS move_type_ref
FROM with_trends
WHERE move_type NOT IN ('INBOUND', 'TRANSFER')
ORDER BY move_type_prefix NULLS LAST

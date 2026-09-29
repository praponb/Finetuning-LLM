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
        unit_price
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, move_type ORDER BY qty NULLS LAST), 3) AS qty_cume_dist,
        DENSE_RANK() OVER (PARTITION BY sku_id, move_type ORDER BY qty NULLS LAST) AS qty_dense_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, warehouse_id), 0), 4) AS unit_price_share_of_move_type_warehouse_id,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, warehouse_id), 2) AS unit_price_vs_move_type_warehouse_id_avg,
        MIN(unit_price) OVER (PARTITION BY move_type) AS move_type_min_unit_price,
        LAG(qty) OVER w_all_by_qty AS prev_qty,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type, warehouse_id)) AS distinct_operator_in_move_type_warehouse_id,
        DENSE_RANK() OVER (ORDER BY unit_price, id) AS unit_price_dense_rank,
        LAG(moved_at) OVER w_all_by_unit_price AS prev_moved_at
    FROM scoped
    WINDOW w_all_by_qty AS (ORDER BY qty, id), w_all_by_unit_price AS (ORDER BY unit_price ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE qty_dense_rank <= 5
      AND move_type IN ('OUTBOUND', 'ADJUST')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY unit_price_share_of_move_type_warehouse_id, id) AS unit_price_share_of_move_type_warehouse_id_quart,
        COUNT(*) OVER w_distinct_operator_in_move_ty AS rows_in_distinct_operator_in_move_type_warehouse,
        SUM(prev_qty) OVER w_distinct_operator_in_move_ty AS distinct_operator_in_move_type_warehouse_id_move,
        prev_qty - LAG(prev_qty) OVER (PARTITION BY unit_price_dense_rank ORDER BY unit_price_share_of_move_type_warehouse_id DESC, id) AS prev_qty_change,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY distinct_operator_in_move_type_warehouse_id, move_type)) AS distinct_operator_in_distinct_operator_in_move_t
    FROM top_per_group
    WINDOW w_distinct_operator_in_move_ty AS (PARTITION BY distinct_operator_in_move_type_warehouse_id, move_type)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    qty_cume_dist,
    qty_dense_rank,
    unit_price_share_of_move_type_warehouse_id,
    unit_price_vs_move_type_warehouse_id_avg,
    move_type_min_unit_price,
    prev_qty,
    distinct_operator_in_move_type_warehouse_id,
    unit_price_dense_rank,
    prev_moved_at,
    rows_in_distinct_operator_in_move_type_warehouse,
    distinct_operator_in_move_type_warehouse_id_move,
    prev_qty_change,
    distinct_operator_in_distinct_operator_in_move_t,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
FROM with_trends
ORDER BY distinct_operator_in_move_type_warehouse_id_move DESC, unit_price_vs_move_type_warehouse_id_avg DESC

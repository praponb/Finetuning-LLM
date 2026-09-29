WITH recent_moves AS (
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
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE qty <= 271
      AND move_type = 'ADJUST'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER w_moved_at_hour) AS distinct_sku_id_in_moved_at_hour,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at, id), 3) AS moved_at_percent_rank,
        MIN(unit_price) OVER (PARTITION BY move_type, operator) AS move_type_operator_min_unit_price,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        MAX(unit_price) OVER (ORDER BY qty ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_max_2,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type, operator)) AS distinct_warehouse_id_in_move_type_operator,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at), 3) AS moved_at_percent_rank_2,
        RANK() OVER w_all_by_qty_ties AS qty_rank,
        FIRST_VALUE(moved_at_hour) OVER (ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at_hour
    FROM recent_moves
    WINDOW w_moved_at_hour AS (PARTITION BY moved_at_hour), w_all_by_qty_ties AS (ORDER BY qty)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at_hour,
        distinct_sku_id_in_moved_at_hour,
        moved_at_percent_rank,
        move_type_operator_min_unit_price,
        second_qty,
        unit_price_max_2,
        distinct_warehouse_id_in_move_type_operator,
        moved_at_percent_rank_2,
        first_moved_at_hour
    FROM metrics
    WHERE qty_rank <= 5
      AND qty BETWEEN 99 AND 288
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY operator, warehouse_id) AS rows_in_operator_warehouse_id,
        DENSE_RANK() OVER (ORDER BY distinct_warehouse_id_in_move_type_operator DESC) AS distinct_warehouse_id_in_move_type_operator_dens,
        FIRST_VALUE(second_qty) OVER (PARTITION BY operator ORDER BY second_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_qty
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    moved_at_hour,
    distinct_sku_id_in_moved_at_hour,
    moved_at_percent_rank,
    move_type_operator_min_unit_price,
    second_qty,
    unit_price_max_2,
    distinct_warehouse_id_in_move_type_operator,
    first_moved_at_hour,
    first_second_qty,
    DATE_TRUNC('MONTH', moved_at) AS moved_at_month
FROM metrics_2
ORDER BY distinct_warehouse_id_in_move_type_operator DESC NULLS FIRST, sku_id DESC NULLS FIRST, id
LIMIT 20

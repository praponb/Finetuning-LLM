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
        unit_price
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR warehouse_id IS NOT NULL)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        moved_at,
        unit_price,
        COUNT(*) OVER (PARTITION BY warehouse_id, operator) AS rows_in_warehouse_id_operator,
        LEAD(unit_price) OVER w_sku_id_by_qty AS next_unit_price,
        SUM(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_sum_unit_price,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_sum_6_prior,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty, id) AS rn,
        MIN(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_min_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id ORDER BY qty ASC, id), 3) AS qty_cume_dist,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY qty ASC, id) AS qty_dense_rank,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, operator)) AS distinct_sku_id_in_warehouse_id_operator,
        MAX(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_max_7,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY qty ASC, id), 3) AS qty_cume_dist_2,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY operator, warehouse_id)) AS distinct_move_type_in_operator_warehouse_id
    FROM recent_moves
    WINDOW w_sku_id_by_qty AS (PARTITION BY sku_id ORDER BY qty, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE qty_dense_rank = 1
)
SELECT
    id,
    move_type,
    moved_at,
    unit_price,
    next_unit_price,
    operator_warehouse_id_sum_unit_price,
    unit_price_sum_6_prior,
    qty_cume_dist,
    second_moved_at,
    qty_dense_rank,
    unit_price_max_7,
    qty_cume_dist_2,
    distinct_move_type_in_operator_warehouse_id,
    CASE WHEN unit_price >= 329.7 THEN 'A' WHEN unit_price >= 152.7 THEN 'B' ELSE 'C' END AS unit_price_tier
FROM top_per_group
WHERE qty_cume_dist_2 > 0.3

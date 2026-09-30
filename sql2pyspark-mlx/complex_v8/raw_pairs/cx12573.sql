WITH base_moves AS (
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
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE qty BETWEEN 85 AND 468
      AND operator IN ('op_dew', 'op_ann')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price_bucket DESC), 3) AS unit_price_bucket_percent_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id, 0), 4) AS unit_price_share_of_sku_id,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY moved_at) AS moved_at_dense_rank,
        ROUND(AVG(qty) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LAST_VALUE(qty) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        MIN(unit_price) OVER (PARTITION BY sku_id) AS sku_id_min_unit_price,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_bucket_share_of_sku_id,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        ROUND(AVG(qty) OVER (ORDER BY unit_price_bucket DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        ROUND(CUME_DIST() OVER w_move_type_warehouse_id_by_un_2, 3) AS unit_price_bucket_cume_dist,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id)) AS distinct_operator_in_sku_id
    FROM base_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_move_type_warehouse_id_by_un_2 AS (PARTITION BY move_type, warehouse_id ORDER BY unit_price_bucket, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_bucket,
    unit_price_bucket_percent_rank,
    unit_price_share_of_sku_id,
    qty_avg_3,
    second_qty,
    sku_id_min_unit_price,
    unit_price_bucket_share_of_sku_id,
    distinct_warehouse_id_in_sku_id,
    qty_avg_1_prior,
    last_known_moved_at,
    distinct_operator_in_sku_id,
    COALESCE(unit_price, 0.0) AS unit_price_filled,
    moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
FROM sequenced
ORDER BY distinct_warehouse_id_in_sku_id DESC, operator

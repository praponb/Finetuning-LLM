WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        FLOOR(qty / 100) * 100 AS qty_bucket_2
    FROM stock_moves
    WHERE qty >= 168
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(qty_bucket, 2) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_bucket,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), sku_id)) AS distinct_move_type_in_moved_at_month_sku_id,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, TO_DATE(moved_at) ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        FIRST_VALUE(moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY qty_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        ROUND(AVG(qty_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), sku_id), 2) AS moved_at_month_sku_id_avg_qty_bucket,
        LAG(qty) OVER w_warehouse_id_moved_at_month_2 AS prev_qty,
        MIN(qty_bucket) OVER (PARTITION BY sku_id) AS sku_id_min_qty_bucket
    FROM valid
    WINDOW w_warehouse_id_moved_at_month_2 AS (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    second_qty_bucket,
    distinct_move_type_in_moved_at_month_sku_id,
    first_moved_at,
    distinct_warehouse_id_in_sku_id,
    moved_at_month_sku_id_avg_qty_bucket,
    prev_qty,
    sku_id_min_qty_bucket,
    DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label
FROM sequenced
WHERE second_qty_bucket <= 325
ORDER BY moved_at DESC NULLS FIRST, id
LIMIT 10

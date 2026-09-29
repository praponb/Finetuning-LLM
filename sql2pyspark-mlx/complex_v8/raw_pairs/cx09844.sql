WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        CASE WHEN qty >= 314 THEN 'HIGH' WHEN qty >= 112 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier
    FROM stock_moves
    WHERE unit_price > 161.4
      AND operator <> 'op_eve'
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.days_since_moved_at,
        s.qty_tier,
        w.city AS warehouse_city,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY sku_id, sku_is_active ORDER BY days_since_moved_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_moved_at_avg_2,
        SUM(days_since_moved_at) OVER (PARTITION BY sku_id, sku_is_active ORDER BY days_since_moved_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_moved_at_sum_3,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, sku_is_active ORDER BY days_since_moved_at), 3) AS days_since_moved_at_cume_dist,
        days_since_moved_at - LAG(days_since_moved_at) OVER (PARTITION BY sku_id, sku_is_active ORDER BY days_since_moved_at, id) AS days_since_moved_at_change
    FROM combined_details_warehouse
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    days_since_moved_at,
    qty_tier,
    warehouse_city,
    sku_labels,
    days_since_moved_at_avg_2,
    days_since_moved_at_sum_3,
    CASE WHEN days_since_moved_at >= 560 THEN 'HIGH' WHEN days_since_moved_at >= 175 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_moved_at_tier,
    CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag
FROM with_trends
WHERE days_since_moved_at_change >= -210
ORDER BY warehouse_city DESC

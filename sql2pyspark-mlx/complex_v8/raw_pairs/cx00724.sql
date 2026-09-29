WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 227) AS qty_distance
    FROM stock_moves
    WHERE qty > 78
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.qty_distance,
        sk.labels AS sku_labels,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM prepared s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.qty_distance,
        s.warehouse_wh_code,
        s.warehouse_city,
        sk.is_active AS sku_is_active,
        sk.sku_code
    FROM with_details_sku s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    qty_distance,
    warehouse_wh_code,
    warehouse_city,
    sku_code,
    INITCAP(TRIM(operator)) AS operator_clean,
    CASE WHEN qty >= 349 THEN 'A' WHEN qty >= 152 THEN 'B' ELSE 'C' END AS qty_tier
FROM joined
ORDER BY qty_distance, sku_id DESC

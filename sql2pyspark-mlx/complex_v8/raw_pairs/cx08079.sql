WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND warehouse_id <= 3
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.warehouse_opened_on,
        s.warehouse_capacity,
        s.warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on_2,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category
    FROM with_parent_warehouse s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    warehouse_opened_on,
    warehouse_city,
    warehouse_wh_code,
    warehouse_opened_on_2,
    sku_is_active,
    sku_category
FROM combined_details
ORDER BY warehouse_id DESC, id DESC

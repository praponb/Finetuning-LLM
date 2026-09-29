WITH cleaned AS (
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
        ABS(unit_price - 189.4) AS unit_price_distance
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR warehouse_id IS NOT NULL)
),
joined_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
),
enriched AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.warehouse_opened_on,
        s.warehouse_city,
        w.wh_code AS warehouse_wh_code_2
    FROM joined_warehouse s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    warehouse_opened_on,
    warehouse_city,
    DATEDIFF(DATE '2025-12-31', warehouse_opened_on) AS days_since_warehouse_opened_on
FROM enriched

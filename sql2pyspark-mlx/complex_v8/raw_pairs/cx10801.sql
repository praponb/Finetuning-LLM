WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
with_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        w.city AS warehouse_city
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    unit_price,
    warehouse_city,
    LOWER(TRIM(operator)) AS operator_clean,
    CONCAT_WS('|', operator, warehouse_city) AS operator_warehouse_city_label
FROM with_details
WHERE qty < 349

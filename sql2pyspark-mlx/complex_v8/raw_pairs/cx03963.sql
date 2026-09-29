WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE unit_price <= 387.1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT id, metric, reading
FROM (
    SELECT id, sku_weight_kg, sku_unit_cost
    FROM combined_details
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (sku_weight_kg, sku_unit_cost)
) unp
ORDER BY id, metric

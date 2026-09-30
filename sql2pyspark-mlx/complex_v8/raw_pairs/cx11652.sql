WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE unit_price < 396.4
      AND sku_id IS NOT NULL
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT id, field, reading
FROM (
    SELECT id, unit_price, sku_weight_kg
    FROM with_details_warehouse
) src
UNPIVOT (
    reading FOR field IN (unit_price, sku_weight_kg)
) unp
ORDER BY id, field

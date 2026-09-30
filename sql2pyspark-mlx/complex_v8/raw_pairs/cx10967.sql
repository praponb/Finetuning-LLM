WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        qty - unit_price AS qty_minus_unit_price,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE (qty >= 89 OR sku_id <= 8)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE qty_minus_unit_price > (SELECT AVG(qty_minus_unit_price) FROM filtered)
),
combined_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.greatest_unit_price_qty,
        s.move_type_ref,
        sk.is_active AS sku_is_active,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM screened s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Subtotals with CUBE
SELECT
    sku_id,
    COALESCE(warehouse_city, 'ALL') AS warehouse_city,
    MAX(greatest_unit_price_qty) AS max_greatest_unit_price_qty,
    COUNT(*) AS row_count,
    SUM(greatest_unit_price_qty) AS total_greatest_unit_price_qty
FROM combined_details
GROUP BY CUBE(sku_id, warehouse_city)

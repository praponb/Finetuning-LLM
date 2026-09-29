WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE unit_price BETWEEN 111.7 AND 307.8
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.days_since_moved_at,
        s.qty_minus_unit_price,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM base_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
screened AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM joined_sku s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND YEAR(w.opened_on) = 2024
        )
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    days_since_moved_at,
    qty_minus_unit_price,
    sku_category,
    sku_weight_kg,
    CONCAT(sku_category, '#', CAST(warehouse_id AS STRING)) AS sku_category_ref
FROM screened
ORDER BY days_since_moved_at DESC, id

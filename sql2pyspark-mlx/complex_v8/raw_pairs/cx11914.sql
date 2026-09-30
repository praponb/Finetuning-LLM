WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.operator_prefix,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    operator_prefix,
    sku_labels,
    sku_unit_cost
FROM enriched_sku
ORDER BY unit_price, warehouse_id DESC

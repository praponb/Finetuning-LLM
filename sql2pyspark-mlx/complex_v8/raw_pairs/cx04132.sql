WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND move_type NOT IN ('ADJUST', 'TRANSFER')
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category = 'Food'
),
annotated AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(sku_category)) AS sku_category_clean
    FROM enriched
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT s.*, sk.is_active AS sku_is_active, sk.weight_kg AS sku_weight_kg
    FROM annotated s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_is_active, warehouse_id, sku_category, qty
    FROM with_details_sku
) src
PIVOT (
    SUM(qty)
    FOR sku_category IN ('Apparel', 'Toys', 'Food')
)

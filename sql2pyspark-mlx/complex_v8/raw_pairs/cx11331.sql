WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE qty <= 349
      AND TO_DATE(moved_at) < DATE '2025-03-08'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.qty_plus_unit_price,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM base s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
labeled AS (
    -- Add calculated columns
    SELECT *, qty + qty_plus_unit_price AS qty_plus_qty_plus_unit_price
    FROM combined_details
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        move_type,
        qty,
        qty_plus_unit_price,
        sku_category,
        sku_is_active,
        qty_plus_qty_plus_unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM labeled
    WHERE move_type NOT IN ('INBOUND', 'ADJUST')
)
-- Final projection
SELECT
    id,
    move_type,
    qty,
    qty_plus_unit_price,
    sku_is_active,
    qty_plus_qty_plus_unit_price,
    qty_bucket,
    CASE WHEN sku_category IN ('Electronics', 'Food') THEN 'group_a' WHEN sku_category = 'Toys' THEN 'group_b' ELSE 'other' END AS sku_category_group
FROM scored
ORDER BY qty_plus_unit_price, id
LIMIT 5

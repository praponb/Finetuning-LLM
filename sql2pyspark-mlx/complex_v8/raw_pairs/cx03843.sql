WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        unit_price + qty AS unit_price_plus_qty,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE operator <> 'op_ann'
      AND qty < 245
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_plus_qty,
        s.operator_len,
        sk.weight_kg AS sku_weight_kg,
        sk.category AS sku_category,
        w.city AS warehouse_city
    FROM cleaned_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        operator,
        unit_price,
        unit_price_plus_qty,
        operator_len,
        sku_weight_kg,
        sku_category,
        warehouse_city,
        LENGTH(TRIM(sku_category)) AS sku_category_len
    FROM with_parent_sku
)
SELECT
    id,
    warehouse_id,
    operator,
    unit_price,
    unit_price_plus_qty,
    operator_len,
    sku_weight_kg,
    sku_category,
    warehouse_city,
    sku_category_len,
    COALESCE(unit_price_plus_qty, 0.0) AS unit_price_plus_qty_filled,
    CONCAT_WS('|', warehouse_city, sku_category) AS warehouse_city_sku_category_label
FROM tagged
ORDER BY warehouse_id DESC NULLS FIRST, id
LIMIT 20

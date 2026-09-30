WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND qty < 275
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.operator_prefix,
        s.move_type_clean,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
enriched_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.operator_prefix,
        s.sku_weight_kg,
        s.sku_code,
        s.warehouse_city,
        s.warehouse_wh_code,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg_2
    FROM with_details_sku s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        sku_weight_kg - sku_weight_kg_2 AS sku_weight_kg_minus_sku_weight_kg_2
    FROM enriched_sku
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    operator_prefix,
    sku_weight_kg,
    warehouse_city,
    warehouse_wh_code,
    sku_weight_kg_2,
    sku_weight_kg_minus_sku_weight_kg_2,
    CONCAT_WS('-', sku_code, sku_category) AS sku_code_sku_category_label,
    moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d
FROM annotated
ORDER BY moved_at_plus_1d NULLS LAST, operator_prefix DESC

WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty <= 372
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM with_activity
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.qty_bucket,
        s.operator_prefix,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg,
        w.wh_code AS warehouse_wh_code
    FROM derived s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty_bucket,
        operator_prefix,
        sku_code,
        sku_labels,
        sku_weight_kg,
        warehouse_wh_code,
        CONCAT(warehouse_wh_code, '#', CAST(sku_id AS STRING)) AS warehouse_wh_code_ref,
        CASE WHEN sku_weight_kg >= 15.2 THEN 'A' WHEN sku_weight_kg >= 12.4 THEN 'B' ELSE 'C' END AS sku_weight_kg_tier
    FROM joined_sku
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty_bucket,
        s.sku_code,
        s.sku_labels,
        s.sku_weight_kg,
        s.warehouse_wh_code_ref,
        s.sku_weight_kg_tier,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg_2,
        sk.is_active AS sku_is_active
    FROM scored s
    JOIN skus sk ON sk.id = s.sku_id
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty_bucket,
        s.sku_code,
        s.sku_weight_kg,
        s.sku_weight_kg_2,
        s.sku_is_active,
        sk.sku_code AS sku_sku_code,
        sk.weight_kg AS sku_weight_kg_3,
        sk.labels AS sku_labels_2,
        w.opened_on AS warehouse_opened_on
    FROM enriched s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty_bucket,
    sku_code,
    sku_weight_kg,
    sku_weight_kg_2,
    sku_is_active,
    sku_sku_code,
    sku_labels_2,
    warehouse_opened_on,
    ROUND(sku_weight_kg_3 * 1.5, 2) AS sku_weight_kg_3_adj,
    FLOOR(qty_bucket / 100) * 100 AS qty_bucket_bucket
FROM combined_details
ORDER BY id

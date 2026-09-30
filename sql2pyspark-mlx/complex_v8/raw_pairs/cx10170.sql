WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        operator_len - qty AS operator_len_minus_qty,
        LOWER(TRIM(operator_or_unknown)) AS operator_or_unknown_clean
    FROM base_moves
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_len,
        s.operator_len_minus_qty,
        sk.labels AS sku_labels,
        sk.sku_code,
        sk.is_active AS sku_is_active
    FROM derived s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
exploded AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_len,
        s.operator_len_minus_qty,
        s.sku_code,
        s.sku_is_active,
        t.sku_label
    FROM enriched_sku s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(sku_label), 1, 3) AS sku_label_prefix,
        LENGTH(TRIM(sku_code)) AS sku_code_len,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM exploded
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    operator,
    operator_len,
    operator_len_minus_qty,
    sku_code,
    sku_label,
    DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
FROM labeled
ORDER BY days_since_moved_at DESC

WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator_prefix,
        s.operator_move_type_label,
        s.move_type_or_unknown,
        sk.sku_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM cleaned s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        operator_prefix,
        operator_move_type_label,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        SUBSTRING(TRIM(move_type_or_unknown), 1, 4) AS move_type_or_unknown_prefix
    FROM combined_details_sku
    WHERE warehouse_opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        operator_move_type_label,
        moved_at_label,
        move_type_or_unknown_prefix,
        SUBSTRING(TRIM(operator_prefix), 1, 3) AS operator_prefix_prefix,
        LOWER(REGEXP_REPLACE(TRIM(moved_at_label), ' ', '_')) AS moved_at_label_slug,
        SUBSTRING(TRIM(operator_prefix), 1, 4) AS operator_prefix_prefix_2
    FROM enriched
)
SELECT
    id,
    warehouse_id,
    move_type_or_unknown_prefix,
    moved_at_label_slug,
    operator_prefix_prefix_2,
    LENGTH(TRIM(operator_prefix_prefix_2)) AS operator_prefix_prefix_2_len,
    CONCAT(moved_at_label_slug, '#', CAST(warehouse_id AS STRING)) AS moved_at_label_slug_ref
FROM labeled
WHERE operator_prefix_prefix IS NOT NULL
ORDER BY move_type_or_unknown_prefix DESC, moved_at_label_slug DESC, id
LIMIT 5

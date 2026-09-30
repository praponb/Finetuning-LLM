WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE operator NOT IN ('op_bee', 'op_ann')
      AND warehouse_id <= 5
),
combined_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.moved_at_label,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        sk.sku_code
    FROM valid s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
        LOWER(REGEXP_REPLACE(TRIM(moved_at_label), ' ', '_')) AS moved_at_label_slug,
        CASE WHEN unit_price >= 361.9 THEN 'HIGH' WHEN unit_price >= 136.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM combined_details_warehouse
    WHERE moved_at_label IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(warehouse_capacity - 11506) AS warehouse_capacity_distance,
        COALESCE(unit_price_tier, 'UNKNOWN') AS unit_price_tier_or_unknown,
        unit_price + warehouse_capacity AS unit_price_plus_warehouse_capacity
    FROM annotated
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.moved_at_label,
        s.warehouse_wh_code,
        s.sku_code,
        s.unit_price_tier_or_unknown,
        s.unit_price_plus_warehouse_capacity,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels
    FROM enriched s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    warehouse_wh_code,
    sku_code,
    unit_price_plus_warehouse_capacity,
    sku_is_active,
    UPPER(TRIM(warehouse_wh_code)) AS warehouse_wh_code_clean,
    CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag
FROM with_parent
ORDER BY warehouse_wh_code_clean, unit_price DESC, id
LIMIT 10

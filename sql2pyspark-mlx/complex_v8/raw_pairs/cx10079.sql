WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        sk.labels AS sku_labels,
        w.opened_on AS warehouse_opened_on
    FROM scoped_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM with_parent_sku
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND LOWER(w.wh_code) LIKE '%ul%'
        )
),
with_parent_sku_2 AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code,
        sk.is_active AS sku_is_active
    FROM without_matches s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost >= 68.0
)
-- Final projection
SELECT
    id,
    warehouse_id,
    unit_price,
    sku_labels,
    warehouse_opened_on,
    unit_price_bucket,
    sku_code,
    CASE WHEN move_type IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
    IF(sku_is_active, 'yes', 'no') AS sku_is_active_label
FROM with_parent_sku_2
ORDER BY warehouse_id, unit_price DESC NULLS FIRST

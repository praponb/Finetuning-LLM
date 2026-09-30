WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE (move_type IN ('ADJUST', 'OUTBOUND', 'TRANSFER') OR warehouse_id IS NOT NULL)
),
joined AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        sk.is_active AS sku_is_active
    FROM recent s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM joined
    WHERE warehouse_capacity < (SELECT MAX(warehouse_capacity) FROM joined)
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.warehouse_opened_on,
        s.warehouse_city,
        s.warehouse_capacity,
        s.sku_is_active,
        sk.is_active AS sku_is_active_2,
        sk.sku_code
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.warehouse_opened_on,
        s.warehouse_capacity,
        s.sku_is_active,
        s.sku_code,
        sk.sku_code AS sku_sku_code,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels
    FROM joined_sku s
    JOIN skus sk ON sk.id = s.sku_id
),
exploded AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.warehouse_opened_on,
        s.warehouse_capacity,
        s.sku_is_active,
        s.sku_code,
        s.sku_sku_code,
        s.sku_weight_kg,
        t.sku_label
    FROM enriched s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    warehouse_opened_on,
    warehouse_capacity,
    sku_code,
    sku_sku_code,
    sku_weight_kg,
    sku_label
FROM exploded
ORDER BY warehouse_capacity DESC, id, sku_label
LIMIT 5

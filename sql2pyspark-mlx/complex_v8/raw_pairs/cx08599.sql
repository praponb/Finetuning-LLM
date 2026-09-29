WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
),
unique_rows AS (
    -- Keep the latest row per move_type, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        moved_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC, id DESC) AS row_num
        FROM prepared_moves
    ) ranked
    WHERE row_num = 1
),
with_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        sk.sku_code,
        sk.labels AS sku_labels
    FROM unique_rows s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
exploded AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.sku_code,
        t.sku_label
    FROM with_details s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN sku_label IN ('express', 'fragile') THEN 'group_a' WHEN sku_label = 'cold' THEN 'group_b' ELSE 'other' END AS sku_label_group,
        LOWER(TRIM(sku_code)) AS sku_code_clean
    FROM exploded
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.sku_code,
        s.sku_label,
        s.sku_code_clean,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity
    FROM labeled s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    sku_code,
    sku_label,
    sku_code_clean,
    warehouse_wh_code,
    warehouse_city,
    warehouse_capacity,
    CASE WHEN warehouse_capacity >= 11506 THEN 'HIGH' WHEN warehouse_capacity >= 4912 THEN 'MEDIUM' ELSE 'LOW' END AS warehouse_capacity_tier,
    DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
FROM with_details_warehouse
ORDER BY warehouse_city DESC, warehouse_capacity DESC NULLS FIRST, id, sku_label
LIMIT 20

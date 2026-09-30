WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE move_type NOT IN ('ADJUST', 'OUTBOUND')
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.operator_prefix,
        s.move_type_operator_label,
        s.days_since_moved_at,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.category AS sku_category
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city = 'Singapore'
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.operator_prefix,
        s.move_type_operator_label,
        s.days_since_moved_at,
        s.warehouse_opened_on,
        s.warehouse_wh_code,
        s.sku_category,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg,
        w.city AS warehouse_city_2,
        w.opened_on AS warehouse_opened_on_2
    FROM enriched_warehouse s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        operator_prefix,
        move_type_operator_label,
        days_since_moved_at,
        warehouse_opened_on,
        sku_unit_cost,
        sku_labels,
        warehouse_city_2,
        warehouse_opened_on_2,
        DATEDIFF(warehouse_opened_on, warehouse_opened_on_2) AS days_warehouse_opened_on_2_to_warehouse_opened_o,
        LENGTH(TRIM(warehouse_city_2)) AS warehouse_city_2_len
    FROM with_details_sku
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, sku_unit_cost, unit_price
    FROM derived
) src
UNPIVOT (
    value FOR field IN (sku_unit_cost, unit_price)
) unp

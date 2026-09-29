WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        INITCAP(TRIM(operator)) AS operator_clean,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_chan', 'op_ann')
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.moved_at_month,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM prepared_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.category = 'Apparel'
),
joined AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at_month,
        s.sku_category,
        w.city AS warehouse_city,
        sk.unit_cost AS sku_unit_cost
    FROM combined_details_sku s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.city = 'Bangkok'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at_month, sku_category, qty
    FROM joined
) src
PIVOT (
    SUM(qty)
    FOR sku_category IN ('Electronics', 'Beauty')
)

WITH filtered_moves AS (
    -- Scope the stock_moves rows
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
    WHERE operator <> 'op_bee'
),
with_parent_sku AS (
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
        sk.category AS sku_category,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        w.city AS warehouse_city
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.category = 'Food'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_city, sku_category, qty
    FROM with_parent_sku
) src
PIVOT (
    SUM(qty)
    FOR sku_category IN ('Toys', 'Food', 'Electronics', 'Beauty')
)
ORDER BY warehouse_city

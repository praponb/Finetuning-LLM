WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        qty - unit_price AS qty_minus_unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE operator <> 'op_ann'
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_hour,
        s.qty_minus_unit_price,
        s.moved_at_day,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    COALESCE(operator, 'ALL') AS operator,
    sku_id,
    GROUPING(operator) AS is_total_operator,
    ROUND(STDDEV(qty_minus_unit_price), 2) AS stddev_qty_minus_unit_price,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
    MIN(moved_at_hour) AS first_moved_at_hour
FROM with_parent_sku
GROUP BY GROUPING SETS ((operator, sku_id), (operator), (sku_id))
ORDER BY operator NULLS LAST, sku_id NULLS LAST

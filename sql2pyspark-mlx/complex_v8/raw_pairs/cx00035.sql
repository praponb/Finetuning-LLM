WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_bee', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE operator <> 'op_chan'
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Subtotals with CUBE
SELECT
    sku_category,
    sku_id,
    MIN(qty) AS min_qty
FROM with_details_sku
GROUP BY CUBE(sku_category, sku_id)
ORDER BY sku_category NULLS LAST, sku_id NULLS LAST

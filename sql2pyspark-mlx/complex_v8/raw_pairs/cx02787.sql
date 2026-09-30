WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 145) AS qty_distance
    FROM stock_moves
    WHERE operator = 'op_eve'
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.qty_distance,
        sk.category AS sku_category
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category = 'Food'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    moved_at,
    unit_price,
    qty_distance,
    sku_category,
    CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
FROM with_details

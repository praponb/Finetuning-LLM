WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        sk.labels AS sku_labels
    FROM valid s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    sku_labels,
    CASE WHEN unit_price >= 317.0 THEN 'HIGH' WHEN unit_price >= 161.4 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
FROM joined_sku

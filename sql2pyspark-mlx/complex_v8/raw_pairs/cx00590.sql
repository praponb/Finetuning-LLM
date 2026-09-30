WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    unit_price
FROM scoped_moves
WHERE unit_price <= 393.3

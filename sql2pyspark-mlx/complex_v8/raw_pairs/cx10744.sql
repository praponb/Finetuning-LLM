WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator
FROM recent
ORDER BY qty

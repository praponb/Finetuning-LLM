WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
)
SELECT
    COALESCE(operator, 'ALL') AS operator,
    sku_id,
    COUNT(DISTINCT id) AS distinct_ids
FROM prepared
GROUP BY GROUPING SETS ((operator, sku_id), (operator), (sku_id), ())

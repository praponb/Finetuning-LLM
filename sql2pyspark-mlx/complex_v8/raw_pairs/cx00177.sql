WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
)
SELECT sku_id AS ref_id, move_type AS category
FROM filtered_moves
WHERE sku_id IS NOT NULL
UNION ALL
SELECT id, operator
FROM stock_moves
WHERE qty < 436
UNION ALL
SELECT capacity, wh_code
FROM warehouses
WHERE capacity > 4912

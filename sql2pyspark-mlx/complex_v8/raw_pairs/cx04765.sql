WITH valid_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        ABS(capacity - 11005) AS capacity_distance
    FROM warehouses
    WHERE city = 'Bangkok'
)
SELECT id
FROM valid_warehouses
WHERE capacity_distance BETWEEN 2000 AND 19000
INTERSECT
SELECT sku_id
FROM stock_moves
WHERE MONTH(moved_at) IN (1, 3)
ORDER BY id DESC

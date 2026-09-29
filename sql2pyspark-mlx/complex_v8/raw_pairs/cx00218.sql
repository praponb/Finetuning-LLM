WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        SUBSTRING(TRIM(city), 1, 3) AS city_prefix
    FROM warehouses
    WHERE city IN ('Bangkok', 'Kuala Lumpur')
      AND YEAR(opened_on) = 2025
)
SELECT id, capacity AS key_id
FROM scoped_warehouses
WHERE city_prefix IS NOT NULL
UNION ALL
SELECT warehouse_id AS id, qty AS key_id
FROM stock_moves
ORDER BY key_id DESC

WITH filtered_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
)
SELECT
    city,
    opened_on,
    wh_code AS category,
    'warehouse' AS record_type
FROM warehouses
UNION ALL
SELECT
    wh_code,
    opened_on,
    city,
    'warehouse_2' AS record_type
FROM warehouses
ORDER BY category, opened_on

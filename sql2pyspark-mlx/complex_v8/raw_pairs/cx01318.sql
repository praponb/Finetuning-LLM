SELECT
    id,
    opened_on,
    wh_code,
    'warehouse' AS record_type
FROM warehouses
WHERE LOWER(wh_code) LIKE '%x1%'
UNION ALL
SELECT
    id,
    opened_on,
    wh_code,
    'warehouse_2' AS record_type
FROM warehouses
UNION ALL
SELECT
    id,
    opened_on,
    city,
    'warehouse_3' AS record_type
FROM warehouses
WHERE city = 'Kuala Lumpur'

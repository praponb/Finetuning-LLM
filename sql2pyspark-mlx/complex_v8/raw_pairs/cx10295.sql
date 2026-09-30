SELECT
    'warehouse' AS record_type,
    city,
    capacity,
    opened_on AS day
FROM warehouses
WHERE capacity < 11768
UNION ALL
SELECT
    'warehouse_2' AS record_type,
    city,
    id,
    opened_on
FROM warehouses
WHERE opened_on >= DATE '2024-11-25'
UNION ALL
SELECT
    'warehouse_3' AS record_type,
    wh_code,
    capacity,
    opened_on
FROM warehouses
WHERE city IN ('Kuala Lumpur', 'Bangkok')
  AND LOWER(wh_code) LIKE '%nx%'

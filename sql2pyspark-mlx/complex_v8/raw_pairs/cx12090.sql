SELECT
    id,
    capacity,
    city,
    'warehouse' AS record_type
FROM warehouses
UNION ALL
SELECT
    capacity,
    id,
    city,
    'warehouse_2' AS record_type
FROM warehouses
WHERE city = 'Kuala Lumpur'
  AND capacity BETWEEN 2614 AND 11506

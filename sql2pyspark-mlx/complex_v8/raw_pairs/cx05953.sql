SELECT
    city AS name,
    capacity AS ref_id,
    opened_on,
    'warehouse' AS src
FROM warehouses
WHERE opened_on < DATE '2025-07-03'
  AND capacity >= 2614
UNION ALL
SELECT
    city,
    id,
    opened_on,
    'warehouse_2' AS src
FROM warehouses
UNION ALL
SELECT
    city,
    capacity,
    opened_on,
    'warehouse_3' AS src
FROM warehouses
WHERE city <> 'Bangkok'
  AND opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-12-17'

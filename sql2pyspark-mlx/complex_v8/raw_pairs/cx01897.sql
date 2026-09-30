SELECT
    'warehouse' AS source,
    city AS name,
    id AS qty,
    opened_on
FROM warehouses
WHERE city IN ('Kuala Lumpur', 'Singapore')
  AND wh_code IS NOT NULL
UNION ALL
SELECT
    'warehouse_2' AS source,
    city AS name,
    id AS qty,
    opened_on
FROM warehouses
UNION ALL
SELECT
    'warehouse_3' AS source,
    city AS name,
    capacity AS qty,
    opened_on
FROM warehouses
WHERE capacity < 11768
  AND opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'

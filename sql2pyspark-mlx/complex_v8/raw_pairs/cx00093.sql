SELECT id AS entity_id
FROM warehouses
WHERE city IN ('Kuala Lumpur', 'Bangkok')
  AND YEAR(opened_on) = 2025
EXCEPT ALL
SELECT id
FROM warehouses

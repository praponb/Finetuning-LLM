SELECT id
FROM warehouses
INTERSECT ALL
SELECT capacity
FROM warehouses
WHERE city IS NOT NULL
  AND opened_on IS NOT NULL
ORDER BY id DESC

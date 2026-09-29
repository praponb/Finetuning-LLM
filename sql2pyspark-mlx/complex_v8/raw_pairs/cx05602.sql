SELECT capacity
FROM warehouses
WHERE capacity < 11768
  AND city = 'Kuala Lumpur'
INTERSECT
SELECT capacity
FROM warehouses

SELECT capacity
FROM warehouses
WHERE LENGTH(TRIM(wh_code)) >= 6
  AND capacity <= 11506
INTERSECT
SELECT id
FROM warehouses

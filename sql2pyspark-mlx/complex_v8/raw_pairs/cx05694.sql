SELECT id, city
FROM warehouses
WHERE capacity > 4912
  AND LOWER(wh_code) LIKE '%ul%'
MINUS
SELECT capacity, wh_code
FROM warehouses
WHERE LENGTH(TRIM(wh_code)) >= 8
  AND city = 'Singapore'

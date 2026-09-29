SELECT id, wh_code AS category
FROM warehouses
WHERE city = 'Kuala Lumpur'
  AND capacity < 11005
INTERSECT ALL
SELECT id, wh_code
FROM warehouses
WHERE city NOT IN ('Kuala Lumpur', 'Singapore')
  AND LENGTH(TRIM(wh_code)) >= 5
ORDER BY category DESC, id

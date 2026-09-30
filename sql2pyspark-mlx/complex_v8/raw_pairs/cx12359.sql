SELECT id AS ref_id, city
FROM warehouses
UNION ALL
SELECT id, city
FROM warehouses
WHERE city <> 'Singapore'
  AND LENGTH(TRIM(wh_code)) >= 8
UNION ALL
SELECT capacity, wh_code
FROM warehouses

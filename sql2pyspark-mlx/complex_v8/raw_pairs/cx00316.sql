SELECT id, city
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
  AND opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'
INTERSECT
SELECT id, city
FROM warehouses
WHERE city <> 'Kuala Lumpur'
ORDER BY id DESC

SELECT city AS category
FROM warehouses
WHERE city IN ('Singapore', 'Kuala Lumpur')
  AND UPPER(TRIM(wh_code)) LIKE 'K%'
INTERSECT
SELECT category
FROM skus
ORDER BY category DESC

SELECT id AS entity_id
FROM skus
WHERE unit_cost > 38.3
EXCEPT ALL
SELECT id AS entity_id
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
  AND city = 'Singapore'
ORDER BY entity_id DESC

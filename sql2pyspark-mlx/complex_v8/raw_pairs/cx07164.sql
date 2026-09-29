SELECT id
FROM skus
WHERE is_active = FALSE
EXCEPT
SELECT id
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
  AND capacity < 11005

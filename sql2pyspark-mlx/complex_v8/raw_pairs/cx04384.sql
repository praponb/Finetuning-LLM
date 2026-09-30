SELECT capacity AS key_id
FROM warehouses
WHERE capacity < 11768
  AND UPPER(TRIM(wh_code)) LIKE 'K%'
EXCEPT ALL
SELECT id AS key_id
FROM warehouses
ORDER BY key_id DESC

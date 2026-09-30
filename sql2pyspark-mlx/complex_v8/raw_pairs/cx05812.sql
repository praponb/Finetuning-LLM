SELECT city AS category, id AS key_id, opened_on AS day
FROM warehouses
UNION ALL
SELECT city AS category, capacity AS key_id, opened_on AS day
FROM warehouses
WHERE wh_code IS NOT NULL
  AND city <> 'Kuala Lumpur'
ORDER BY category DESC, key_id

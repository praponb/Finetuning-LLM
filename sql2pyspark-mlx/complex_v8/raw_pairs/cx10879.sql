SELECT capacity AS key_id, id, 'warehouse' AS src
FROM warehouses
UNION ALL
SELECT capacity AS key_id, id, 'warehouse_2' AS src
FROM warehouses
WHERE capacity <= 11768
  AND city <> 'Singapore'

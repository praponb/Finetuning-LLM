SELECT
    capacity AS ref_id,
    opened_on,
    id AS key_id,
    'warehouse' AS record_type
FROM warehouses
UNION ALL
SELECT
    capacity AS ref_id,
    opened_on,
    id AS key_id,
    'warehouse_2' AS record_type
FROM warehouses
WHERE LOWER(wh_code) LIKE '%x1%'
  AND capacity < 11506
ORDER BY ref_id, key_id

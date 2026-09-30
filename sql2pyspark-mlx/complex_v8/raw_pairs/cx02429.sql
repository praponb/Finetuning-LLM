SELECT city, opened_on, 'warehouse' AS record_type
FROM warehouses
UNION ALL
SELECT city, opened_on, 'warehouse_2' AS record_type
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
UNION ALL
SELECT wh_code, opened_on, 'warehouse_3' AS record_type
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
ORDER BY city, record_type

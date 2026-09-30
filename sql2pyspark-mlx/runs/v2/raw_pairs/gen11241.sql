SELECT id, city, UPPER(TRIM(supplier_name)) AS supplier_name_normalized
FROM suppliers
WHERE rating < 2.5
ORDER BY supplier_name_normalized;

SELECT *, LENGTH(supplier_name) AS supplier_name_length, TRIM(supplier_name) AS clean_supplier_name
FROM suppliers
ORDER BY supplier_name_length

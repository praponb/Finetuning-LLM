SELECT id, city, TRIM(supplier_name) AS clean_supplier_name
FROM suppliers
ORDER BY city, clean_supplier_name, id
LIMIT 50

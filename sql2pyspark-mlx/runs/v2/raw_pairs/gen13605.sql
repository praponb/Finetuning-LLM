SELECT *, COALESCE(supplier_name, 'N/A') AS supplier_name_filled, CONCAT_WS('-', city, supplier_name) AS city_supplier_name_key
FROM suppliers
WHERE rating <= 3.8
ORDER BY country DESC, id
LIMIT 10;

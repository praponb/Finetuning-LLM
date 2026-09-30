SELECT *, LENGTH(city) AS city_length, UPPER(supplier_name) AS supplier_name_upper
FROM suppliers
WHERE supplier_name LIKE '%Global%' AND supplier_name LIKE 'Eu%';

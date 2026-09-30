SELECT id, city, UPPER(TRIM(supplier_name)) AS supplier_name_normalized
FROM suppliers
WHERE rating >= 3.5 AND country = 'DE' AND rating BETWEEN 2.6 AND 3.5;

SELECT id, city, CONCAT_WS(',', country, supplier_name) AS country_supplier_name_key
FROM suppliers
WHERE rating > 3.3 OR supplier_name LIKE '%Gmb%'
ORDER BY id DESC;

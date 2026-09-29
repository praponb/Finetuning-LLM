SELECT id, country, CONCAT_WS('/', city, supplier_name) AS city_supplier_name_key
FROM suppliers
ORDER BY id;

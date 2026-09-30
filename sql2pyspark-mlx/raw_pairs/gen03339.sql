SELECT id, rating, country, CONCAT_WS('/', city, supplier_name) AS city_supplier_name_key FROM suppliers WHERE rating IS NULL ORDER BY city_supplier_name_key, id LIMIT 10;

SELECT id, rating, city, supplier_name, CONCAT_WS('/', country, city) AS country_city_key FROM suppliers

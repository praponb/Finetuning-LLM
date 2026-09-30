SELECT rating, supplier_name, city, CONCAT_WS(',', city, supplier_name) AS city_supplier_name_key FROM suppliers WHERE country IN ('UK', 'DE')

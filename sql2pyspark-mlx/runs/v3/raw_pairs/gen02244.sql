SELECT id, country, rating, city, CONCAT_WS(',', supplier_name, country) AS supplier_name_country_key FROM suppliers WHERE rating IS NULL AND LENGTH(supplier_name) > 14

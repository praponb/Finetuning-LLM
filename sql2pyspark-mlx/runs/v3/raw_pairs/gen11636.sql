SELECT supplier_name, CONCAT_WS('-', country, city) country_city_key FROM suppliers WHERE rating BETWEEN 2.2 AND 2.6

SELECT *, CONCAT(city, ' - ', store_name) AS city_store_name, LOWER(city) AS city_lower FROM stores WHERE NOT (LENGTH(store_name) < 13 AND city = 'London') ORDER BY opened_date;

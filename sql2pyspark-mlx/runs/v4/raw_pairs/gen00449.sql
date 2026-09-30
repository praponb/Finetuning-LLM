SELECT id, rating, supplier_name, city, CONCAT(city, ', ', country) AS city_country FROM suppliers WHERE supplier_name IS NULL AND city = 'Bangkok' ORDER BY city_country;

SELECT id, rating, country, city, INITCAP(TRIM(city)) AS city_title FROM suppliers WHERE LENGTH(supplier_name) >= 11 ORDER BY city, rating DESC;

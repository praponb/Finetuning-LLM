SELECT id, rating, city, country, INITCAP(TRIM(city)) AS city_title FROM suppliers WHERE supplier_name LIKE '%Sia%' AND supplier_name NOT LIKE '%Glob%' ORDER BY city, rating DESC;

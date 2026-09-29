SELECT city, rating, country, UPPER(country) AS country_upper FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%' ORDER BY city

SELECT id, city, supplier_name, country, UPPER(country) AS country_upper FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%' AND rating BETWEEN 3.1 AND 3.8;

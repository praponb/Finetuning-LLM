SELECT id, rating, supplier_name, LOWER(country) AS country_lower FROM suppliers WHERE rating <= 2.4 ORDER BY id;

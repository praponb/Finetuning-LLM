SELECT city, MIN(rating) AS min_rating FROM suppliers WHERE supplier_name IS NOT NULL OR LOWER(supplier_name) LIKE '%ion%' GROUP BY city;

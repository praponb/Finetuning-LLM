SELECT *, ROUND(rating, 1) AS rating_rounded, LENGTH(country) AS country_length FROM suppliers WHERE LENGTH(supplier_name) > 8 ORDER BY supplier_name DESC, id LIMIT 1;

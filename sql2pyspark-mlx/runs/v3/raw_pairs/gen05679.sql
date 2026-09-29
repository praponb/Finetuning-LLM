SELECT city, country, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE supplier_name LIKE 'E%'

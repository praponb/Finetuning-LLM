SELECT country, rating, supplier_name, COALESCE(rating, 0) AS rating_filled FROM suppliers ORDER BY rating

SELECT supplier_name, city, COALESCE(rating, 0) AS rating_filled, FLOOR(rating) AS rating_whole FROM suppliers WHERE city NOT IN ('Berlin', 'Tokyo') AND city = 'Chiang Mai' ORDER BY rating_whole;

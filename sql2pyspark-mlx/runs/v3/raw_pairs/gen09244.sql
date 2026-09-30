SELECT id, country, rating, supplier_name, FLOOR(rating) AS rating_whole, ROUND(rating * 1.1, 2) AS rating_adjusted FROM suppliers;

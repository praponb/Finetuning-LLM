SELECT id, city, rating, country, CEIL(rating) AS rating_whole FROM suppliers WHERE country NOT IN ('DE', 'SG', 'US') ORDER BY rating_whole, rating;

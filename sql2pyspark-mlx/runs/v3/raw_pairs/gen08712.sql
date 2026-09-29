SELECT id, country, rating, LEAD(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) next_rating FROM suppliers;

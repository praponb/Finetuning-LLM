SELECT id, country, rating, LEAD(rating, 2, 0) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating FROM suppliers WHERE rating > 2.2;

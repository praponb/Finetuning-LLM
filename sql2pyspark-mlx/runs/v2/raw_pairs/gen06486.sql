SELECT id, country, rating, RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_rank FROM suppliers WHERE rating > 2.1

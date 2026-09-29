SELECT id, country, city, rating, RANK() OVER (PARTITION BY country, city ORDER BY rating DESC) rating_rank FROM suppliers ORDER BY country

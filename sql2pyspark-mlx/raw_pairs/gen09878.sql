SELECT id, city, rating, NTILE(2) OVER (PARTITION BY city ORDER BY rating DESC, id) AS quartile, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rating_rank FROM suppliers ORDER BY quartile;

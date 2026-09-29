SELECT id, country, rating, rating - LAG(rating, 1, 0) OVER (PARTITION BY country ORDER BY rating, id) AS rating_change FROM suppliers ORDER BY rating_change, id DESC;

SELECT id, city, rating, rating - LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS rating_change FROM suppliers ORDER BY id DESC, rating_change DESC;

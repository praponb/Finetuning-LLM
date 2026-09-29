SELECT id, city, rating, LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) prev_rating FROM suppliers ORDER BY id DESC, rating;

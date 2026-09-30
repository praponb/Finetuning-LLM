SELECT id, city, rating, SUM(rating) OVER (PARTITION BY city ORDER BY rating, id) AS running_rating FROM suppliers;

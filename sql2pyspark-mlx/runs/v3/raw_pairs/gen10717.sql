SELECT id, city, rating, LEAD(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) next_rating FROM suppliers

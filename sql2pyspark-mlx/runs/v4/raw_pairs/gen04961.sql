SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rn, LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating FROM suppliers

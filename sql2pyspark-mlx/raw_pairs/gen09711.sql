SELECT id, city, rating, MIN(rating) OVER (PARTITION BY city) AS city_min_rating FROM suppliers ORDER BY city ASC

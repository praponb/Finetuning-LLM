SELECT ROUND(STDDEV(rating), 2) AS stddev_rating, MAX(rating) - MIN(rating) AS rating_range, MIN(rating) AS min_rating
FROM suppliers;

SELECT city, ROUND(STDDEV(rating), 2) stddev_rating, COUNT(rating) rating_count FROM suppliers GROUP BY city;

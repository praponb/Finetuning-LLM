SELECT city, COUNT(DISTINCT rating) AS unique_citys, ROUND(STDDEV(rating), 2) AS stddev_rating FROM suppliers GROUP BY city;

SELECT city, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
WHERE rating <= 2.9
GROUP BY city;

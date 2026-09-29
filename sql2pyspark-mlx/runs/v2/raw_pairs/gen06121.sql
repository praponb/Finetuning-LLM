SELECT city, ROUND(AVG(rating), 2) avg_rating
FROM suppliers
GROUP BY city;

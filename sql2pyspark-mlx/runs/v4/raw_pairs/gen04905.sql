SELECT city, country, rating
FROM suppliers
WHERE rating < 2.4 AND rating < 3.2
ORDER BY country DESC, rating;

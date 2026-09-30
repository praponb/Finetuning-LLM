SELECT country, city, SUM(rating) AS total_rating
FROM suppliers
WHERE rating IS NOT NULL
GROUP BY country, city
HAVING SUM(rating) < 17;

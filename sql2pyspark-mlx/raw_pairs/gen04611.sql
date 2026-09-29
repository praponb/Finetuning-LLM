SELECT country, COUNT(rating) rating_count, ROUND(AVG(rating), 2) avg_rating
FROM suppliers
WHERE city = 'Tokyo'
GROUP BY country
HAVING rating_count < 3
ORDER BY avg_rating DESC;

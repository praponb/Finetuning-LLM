SELECT country, SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating
FROM suppliers
WHERE rating <= 1.8
GROUP BY country
HAVING MIN(rating) > 3.5;

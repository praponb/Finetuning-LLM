SELECT country, city, MIN(rating) AS min_rating
FROM suppliers
GROUP BY country, city
HAVING SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) < 32
ORDER BY min_rating DESC;

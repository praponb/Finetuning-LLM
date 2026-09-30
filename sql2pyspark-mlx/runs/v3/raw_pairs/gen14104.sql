SELECT country, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
GROUP BY country
ORDER BY avg_rating ASC, country ASC
LIMIT 1;

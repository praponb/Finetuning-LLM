SELECT country, MIN(rating) AS min_rating
FROM suppliers
GROUP BY country
ORDER BY country;

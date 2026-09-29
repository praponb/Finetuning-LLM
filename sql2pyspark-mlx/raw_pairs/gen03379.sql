SELECT country, MAX(rating) AS max_rating FROM suppliers GROUP BY country ORDER BY country DESC

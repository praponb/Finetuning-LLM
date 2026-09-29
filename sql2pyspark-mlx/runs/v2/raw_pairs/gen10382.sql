SELECT country, SUM(rating) AS total_rating FROM suppliers GROUP BY country ORDER BY country DESC

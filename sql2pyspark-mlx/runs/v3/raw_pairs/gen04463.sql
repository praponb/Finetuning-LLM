SELECT country, ROUND(AVG(rating), 2) AS avg_rating FROM suppliers GROUP BY 1 HAVING avg_rating > 20 ORDER BY avg_rating DESC, country;

SELECT country, ROUND(AVG(rating), 2) avg_rating, COUNT(rating) rating_count, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) bangkok_count FROM suppliers GROUP BY country ORDER BY avg_rating ASC;

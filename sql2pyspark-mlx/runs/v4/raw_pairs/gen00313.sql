SELECT country, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count, AVG(rating) AS avg_rating FROM suppliers GROUP BY country ORDER BY avg_rating DESC

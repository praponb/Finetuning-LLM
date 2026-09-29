SELECT country, COUNT(*) cnt, MAX(rating) - MIN(rating) rating_range FROM suppliers GROUP BY country HAVING rating_range >= 50 ORDER BY country

SELECT country, COUNT(rating) AS rating_count FROM suppliers GROUP BY country ORDER BY rating_count DESC;

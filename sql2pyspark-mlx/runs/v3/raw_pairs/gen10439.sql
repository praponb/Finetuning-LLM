SELECT * FROM (SELECT country, SUM(rating) AS total_rating, MAX(rating) - MIN(rating) AS rating_range FROM suppliers GROUP BY country) AS t WHERE rating_range > 46 ORDER BY country DESC

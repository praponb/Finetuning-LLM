SELECT country, MAX(rating) AS max_rating FROM suppliers GROUP BY country HAVING MAX(rating) >= 3.8 ORDER BY max_rating DESC;

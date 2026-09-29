SELECT city, MIN(rating) AS min_rating, ROUND(AVG(rating), 2) AS avg_rating, COUNT(DISTINCT rating) AS unique_countrys FROM suppliers WHERE country = 'JP' GROUP BY city ORDER BY unique_countrys DESC;

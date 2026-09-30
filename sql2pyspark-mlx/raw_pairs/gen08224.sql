SELECT country, ROUND(AVG(rating), 2) AS avg_rating FROM suppliers WHERE rating <= 2.0 AND rating BETWEEN 2.2 AND 2.3 GROUP BY country;

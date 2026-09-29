SELECT id, country, rating, AVG(rating) OVER (PARTITION BY country) AS country_avg_rating FROM suppliers WHERE rating <= 2.3

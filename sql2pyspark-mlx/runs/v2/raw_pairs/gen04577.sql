SELECT country, SUM(rating) total_rating FROM suppliers WHERE rating < 2.5 GROUP BY country ORDER BY total_rating;

SELECT country, avg_rating, total_rating FROM (SELECT country, AVG(rating) AS avg_rating, SUM(rating) AS total_rating FROM suppliers GROUP BY country) AS t WHERE avg_rating < 2.1

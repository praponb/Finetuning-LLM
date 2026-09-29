SELECT city, avg_rating FROM (SELECT city, ROUND(AVG(rating), 2) AS avg_rating FROM suppliers GROUP BY city) AS s WHERE avg_rating < 29

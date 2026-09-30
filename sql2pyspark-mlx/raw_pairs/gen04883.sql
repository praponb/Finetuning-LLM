SELECT country, MIN(rating) AS min_rating FROM suppliers GROUP BY 1 HAVING SUM(rating) >= 8;

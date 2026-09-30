SELECT * FROM (SELECT city, MIN(rating) AS min_rating FROM suppliers GROUP BY city) AS t WHERE min_rating < 1.9;

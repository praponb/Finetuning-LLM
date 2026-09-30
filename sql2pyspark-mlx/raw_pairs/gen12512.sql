SELECT city, min_rating FROM (SELECT city, MIN(rating) AS min_rating FROM suppliers GROUP BY city) AS sub WHERE min_rating >= 1.9;

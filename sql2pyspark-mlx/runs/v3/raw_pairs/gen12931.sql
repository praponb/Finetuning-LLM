SELECT * FROM (SELECT city, SUM(CASE WHEN country = 'US' THEN rating ELSE 0 END) AS us_rating FROM suppliers GROUP BY city) AS sub WHERE us_rating < 65 ORDER BY us_rating;

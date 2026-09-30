SELECT SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count, ROUND(AVG(rating), 2) AS avg_rating FROM suppliers;

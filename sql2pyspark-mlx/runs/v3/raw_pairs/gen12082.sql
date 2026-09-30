SELECT SUM(CASE WHEN country = 'US' THEN age ELSE 0 END) AS us_age
FROM customers
ORDER BY us_age DESC;

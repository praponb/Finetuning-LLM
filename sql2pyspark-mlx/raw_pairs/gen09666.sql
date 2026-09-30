SELECT is_active, country, MIN(age) AS min_age
FROM customers
GROUP BY 1, 2
ORDER BY min_age DESC, 2 DESC

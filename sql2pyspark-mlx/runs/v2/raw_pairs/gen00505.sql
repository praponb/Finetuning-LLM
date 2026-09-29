SELECT city, segment, MIN(age) AS min_age FROM customers GROUP BY city, segment ORDER BY city, min_age DESC;

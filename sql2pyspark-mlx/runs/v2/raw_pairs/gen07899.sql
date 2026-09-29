SELECT city, MAX(age) AS max_age, MAX(age) - MIN(age) AS age_range
FROM customers
GROUP BY 1;

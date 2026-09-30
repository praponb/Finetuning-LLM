SELECT city
FROM customers
UNION ALL
SELECT location
FROM departments
ORDER BY city;

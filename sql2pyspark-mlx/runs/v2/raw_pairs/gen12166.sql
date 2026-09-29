SELECT city, COUNT(age) age_count
FROM customers
WHERE email IS NOT NULL AND country <> 'US'
GROUP BY city
ORDER BY 1;

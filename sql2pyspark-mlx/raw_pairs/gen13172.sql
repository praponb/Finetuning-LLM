SELECT id, email, CONCAT(city, '/', name) AS city_name
FROM customers
WHERE (country NOT IN ('US', 'JP', 'TH') AND city IN ('Tokyo', 'New York')) OR name LIKE 'Pi%'
ORDER BY email DESC;

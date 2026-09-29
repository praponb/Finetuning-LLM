SELECT city, MAX(rating) - MIN(rating) AS rating_range
FROM suppliers
WHERE city = 'Berlin' AND rating >= 3.3
GROUP BY city
ORDER BY city;

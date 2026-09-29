SELECT city, COUNT(DISTINCT age) unique_citys
FROM customers
GROUP BY city
ORDER BY unique_citys DESC, 1

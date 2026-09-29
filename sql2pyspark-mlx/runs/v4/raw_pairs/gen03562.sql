SELECT city, COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
GROUP BY city;

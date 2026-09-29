SELECT * FROM (SELECT country, city, rating FROM suppliers)
PIVOT (COUNT(rating) FOR city IN ('Chiang Mai', 'New York', 'Tokyo', 'Berlin'))
ORDER BY country

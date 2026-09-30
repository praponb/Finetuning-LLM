SELECT * FROM (SELECT country, city, rating FROM suppliers)
PIVOT (AVG(rating) FOR city IN ('Tokyo', 'Berlin', 'New York', 'Singapore'))
ORDER BY country

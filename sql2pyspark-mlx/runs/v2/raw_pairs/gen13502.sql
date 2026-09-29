SELECT * FROM (SELECT country, city, rating FROM suppliers)
PIVOT (MAX(rating) FOR city IN ('Tokyo', 'Bangkok', 'Singapore'))
ORDER BY country

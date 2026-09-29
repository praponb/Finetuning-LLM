SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE country <> 'JP')
PIVOT (SUM(rating) FOR city IN ('Bangkok', 'London', 'Singapore'))
ORDER BY country

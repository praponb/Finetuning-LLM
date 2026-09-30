SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%')
PIVOT (AVG(rating) FOR city IN ('London', 'Tokyo', 'Singapore'))
ORDER BY country

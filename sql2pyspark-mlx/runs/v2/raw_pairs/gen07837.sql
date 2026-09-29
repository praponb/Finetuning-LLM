SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE supplier_name LIKE '%Pac%')
PIVOT (MAX(rating) FOR city IN ('Singapore', 'Chiang Mai', 'New York'))
ORDER BY country;

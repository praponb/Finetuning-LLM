SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE supplier_name LIKE '%Tra%')
PIVOT (AVG(rating) FOR city IN ('Bangkok', 'New York', 'Chiang Mai'));

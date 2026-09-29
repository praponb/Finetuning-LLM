SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE country = 'US') PIVOT (SUM(rating) FOR city IN ('Bangkok', 'Berlin'));

SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE rating > 2.2)
PIVOT (SUM(rating) FOR city IN ('Berlin', 'New York'));

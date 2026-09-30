SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE rating <= 4.0)
PIVOT (MAX(rating) FOR city IN ('Chiang Mai', 'Bangkok', 'New York', 'Singapore'))

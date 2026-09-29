SELECT * FROM (SELECT country, city, rating FROM suppliers) PIVOT (AVG(rating) FOR city IN ('Berlin', 'London', 'Tokyo'));

SELECT * FROM (SELECT segment, city, age FROM customers) src PIVOT (MAX(age) FOR city IN ('Berlin', 'London'))

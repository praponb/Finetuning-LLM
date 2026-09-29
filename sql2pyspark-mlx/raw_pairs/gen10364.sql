SELECT * FROM (SELECT country, city, segment, age FROM customers) src PIVOT (MAX(age) AS value_sum, COUNT(1) AS n FOR segment IN ('SMB', 'Consumer'))

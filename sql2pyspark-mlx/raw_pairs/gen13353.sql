SELECT * FROM (SELECT country, city, age FROM customers) PIVOT (AVG(age) FOR city IN ('London', 'Chiang Mai', 'Bangkok')) ORDER BY country

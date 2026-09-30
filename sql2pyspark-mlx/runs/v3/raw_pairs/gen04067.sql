SELECT * FROM (SELECT segment, city, age FROM customers) PIVOT (AVG(age) FOR city IN ('London', 'Bangkok', 'Berlin')) ORDER BY segment;

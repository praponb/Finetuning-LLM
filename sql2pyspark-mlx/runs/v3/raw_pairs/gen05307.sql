SELECT * FROM (SELECT segment, city, age FROM customers) PIVOT (MAX(age) FOR city IN ('New York', 'Berlin', 'London')) ORDER BY segment

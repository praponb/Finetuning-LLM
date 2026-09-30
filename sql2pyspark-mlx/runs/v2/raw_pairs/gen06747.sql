SELECT * FROM (SELECT is_active, city, age FROM customers WHERE age BETWEEN 54 AND 67) PIVOT (AVG(age) FOR city IN ('Tokyo', 'Chiang Mai', 'Singapore', 'London')) ORDER BY is_active

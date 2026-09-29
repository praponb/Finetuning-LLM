SELECT * FROM (SELECT country, city, age FROM customers) PIVOT (COUNT(age) FOR city IN ('New York', 'Singapore')) ORDER BY country;

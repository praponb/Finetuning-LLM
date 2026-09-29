SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE rating <= 2.4) PIVOT (SUM(rating) FOR city IN ('New York', 'Singapore')) ORDER BY country;

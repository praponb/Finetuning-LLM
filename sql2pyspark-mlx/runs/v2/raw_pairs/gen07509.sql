SELECT * FROM (SELECT country, city, credit_limit FROM customers) PIVOT (COUNT(credit_limit) FOR city IN ('Singapore', 'New York')) ORDER BY country;

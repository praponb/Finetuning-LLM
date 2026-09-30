SELECT * FROM (SELECT segment, city, credit_limit FROM customers) PIVOT (AVG(credit_limit) FOR city IN ('Berlin', 'London', 'Singapore')) ORDER BY segment;

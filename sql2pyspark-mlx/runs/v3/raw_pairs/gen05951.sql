SELECT * FROM (SELECT segment, city, credit_limit FROM customers WHERE city IS NULL) PIVOT (SUM(credit_limit) FOR city IN ('Berlin', 'Bangkok')) ORDER BY segment;

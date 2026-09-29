SELECT * FROM (SELECT country, city, credit_limit FROM customers) PIVOT (AVG(credit_limit) FOR city IN ('Chiang Mai', 'Bangkok', 'Tokyo')) ORDER BY country;

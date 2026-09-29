SELECT * FROM (SELECT country, segment, age FROM customers) PIVOT (COUNT(age) FOR segment IN ('SMB', 'Consumer')) ORDER BY country;

SELECT * FROM (SELECT segment, country, age FROM customers) PIVOT (COUNT(age) FOR country IN ('DE', 'SG', 'US', 'JP')) ORDER BY segment

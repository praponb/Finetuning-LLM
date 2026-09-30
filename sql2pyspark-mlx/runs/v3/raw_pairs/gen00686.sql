SELECT * FROM (SELECT segment, country, age FROM customers) PIVOT (COUNT(age) FOR country IN ('SG', 'JP')) ORDER BY segment

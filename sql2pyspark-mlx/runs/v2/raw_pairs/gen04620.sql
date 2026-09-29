SELECT * FROM (SELECT city, country, age FROM customers) PIVOT (AVG(age) FOR country IN ('TH', 'DE', 'JP')) ORDER BY city

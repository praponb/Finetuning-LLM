SELECT * FROM (SELECT city, country, rating FROM suppliers) PIVOT (SUM(rating) FOR country IN ('DE', 'SG')) ORDER BY city

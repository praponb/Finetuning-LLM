SELECT * FROM (SELECT city, country, age FROM customers) PIVOT (SUM(age) FOR country IN ('DE', 'TH', 'UK'))

SELECT * FROM (SELECT city, country, age FROM customers) PIVOT (AVG(age) FOR country IN ('JP', 'DE', 'SG'))

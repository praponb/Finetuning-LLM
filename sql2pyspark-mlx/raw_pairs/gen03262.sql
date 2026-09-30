SELECT * FROM (SELECT is_active, city, age FROM customers WHERE YEAR(signup_date) >= 2024) PIVOT (SUM(age) FOR city IN ('New York', 'Bangkok'))

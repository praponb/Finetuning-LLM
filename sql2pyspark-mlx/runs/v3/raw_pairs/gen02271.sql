SELECT * FROM (SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, city, age FROM customers)
PIVOT (COUNT(age) FOR city IN ('Chiang Mai', 'Bangkok'))
ORDER BY signup_month

SELECT * FROM (SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, city, age FROM customers)
PIVOT (COUNT(age) FOR city IN ('Singapore', 'Chiang Mai', 'Bangkok', 'Tokyo'))

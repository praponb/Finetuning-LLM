SELECT * FROM (SELECT MONTH(signup_date) AS signup_month, country, age FROM customers)
PIVOT (COUNT(age) FOR country IN ('UK', 'JP', 'TH', 'DE'))

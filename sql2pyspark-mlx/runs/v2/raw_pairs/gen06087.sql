SELECT * FROM (SELECT MONTH(signup_date) AS signup_month, country, age FROM customers)
PIVOT (AVG(age) FOR country IN ('SG', 'UK', 'TH'));

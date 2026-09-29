SELECT * FROM (SELECT segment, country, age FROM customers WHERE credit_limit < 10500)
PIVOT (SUM(age) FOR country IN ('UK', 'TH'));

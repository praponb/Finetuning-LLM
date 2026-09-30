SELECT * FROM (SELECT segment, country, credit_limit FROM customers WHERE country != 'US')
PIVOT (SUM(credit_limit) FOR country IN ('UK', 'US'))
ORDER BY segment;

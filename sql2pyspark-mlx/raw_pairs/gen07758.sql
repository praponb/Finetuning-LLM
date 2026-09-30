SELECT * FROM (SELECT city, segment, credit_limit FROM customers WHERE country = 'TH')
PIVOT (SUM(credit_limit) FOR segment IN ('Corporate', 'SMB'))

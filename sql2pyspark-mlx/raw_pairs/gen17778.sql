SELECT * FROM (SELECT city, segment, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR segment IN ('Corporate', 'SMB'))
ORDER BY city;

SELECT * FROM (SELECT city, segment, credit_limit FROM customers)
PIVOT (MAX(credit_limit) FOR segment IN ('SMB', 'Corporate'))

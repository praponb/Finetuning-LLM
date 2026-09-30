SELECT * FROM (SELECT city, segment, credit_limit FROM customers WHERE is_active = TRUE)
PIVOT (MAX(credit_limit) FOR segment IN ('SMB', 'Corporate', 'Consumer'))
ORDER BY city

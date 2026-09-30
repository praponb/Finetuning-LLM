SELECT * FROM (SELECT country, segment, credit_limit FROM customers WHERE credit_limit <= 5200) PIVOT (COUNT(credit_limit) FOR segment IN ('Corporate', 'SMB', 'Consumer'));

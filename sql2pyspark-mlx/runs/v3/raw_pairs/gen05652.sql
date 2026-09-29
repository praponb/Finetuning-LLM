SELECT * FROM (SELECT QUARTER(signup_date) AS signup_quarter, segment, credit_limit FROM customers) PIVOT (AVG(credit_limit) FOR segment IN ('SMB', 'Consumer'))

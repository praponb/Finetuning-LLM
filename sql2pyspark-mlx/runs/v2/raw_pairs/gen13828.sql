SELECT * FROM (SELECT YEAR(signup_date) AS signup_year, segment, credit_limit FROM customers) PIVOT (MAX(credit_limit) FOR segment IN ('SMB', 'Consumer', 'Corporate'));

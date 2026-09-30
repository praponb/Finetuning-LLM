SELECT * FROM (SELECT segment, country, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR country IN ('TH', 'JP', 'UK'));

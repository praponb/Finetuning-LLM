SELECT id, city, credit_limit, country, DATE_SUB(signup_date, 37) AS signup_date_minus_37d FROM customers WHERE segment IN ('Corporate', 'SMB', 'Consumer');

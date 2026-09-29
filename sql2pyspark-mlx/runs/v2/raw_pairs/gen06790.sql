SELECT id, credit_limit, signup_date
FROM customers
WHERE credit_limit >= (SELECT MAX(credit_limit) FROM customers WHERE segment = 'Consumer')

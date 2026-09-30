SELECT email FROM customers WHERE credit_limit < 8600 EXCEPT SELECT email FROM employees ORDER BY email

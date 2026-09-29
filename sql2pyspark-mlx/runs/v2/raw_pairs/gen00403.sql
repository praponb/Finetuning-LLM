SELECT email
FROM customers
EXCEPT
SELECT email
FROM employees

SELECT email, 'customer' AS kind
FROM customers
UNION
SELECT email, 'employee' AS kind
FROM employees
ORDER BY email

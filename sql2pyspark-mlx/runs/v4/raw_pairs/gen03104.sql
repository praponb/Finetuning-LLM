SELECT email
FROM customers
WHERE country = 'US'
EXCEPT
SELECT email
FROM employees
WHERE full_name LIKE 'FR%'

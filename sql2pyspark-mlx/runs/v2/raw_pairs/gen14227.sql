SELECT email
FROM customers
WHERE name LIKE 'Ja%'
UNION ALL
SELECT email
FROM employees

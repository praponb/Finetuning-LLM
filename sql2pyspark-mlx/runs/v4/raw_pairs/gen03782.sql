SELECT email
FROM customers
WHERE segment = 'Corporate'
UNION
SELECT email
FROM employees
ORDER BY email;

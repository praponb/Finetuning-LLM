SELECT email
FROM customers
WHERE NOT is_active
UNION
SELECT email
FROM employees
WHERE salary <= 79000
ORDER BY email DESC;

SELECT 'customer' source, email
FROM customers
WHERE age IS NULL
UNION
SELECT 'employee' source, email
FROM employees
WHERE LOWER(full_name) LIKE '%oah%';

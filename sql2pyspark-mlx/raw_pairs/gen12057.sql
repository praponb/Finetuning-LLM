SELECT email
FROM customers
UNION ALL
SELECT email
FROM employees
WHERE is_remote = TRUE;

SELECT name, 'customer' AS source
FROM customers
UNION ALL
SELECT full_name, 'employee' AS source
FROM employees
ORDER BY source;

SELECT name label
FROM customers
UNION
SELECT full_name label
FROM employees
ORDER BY label;

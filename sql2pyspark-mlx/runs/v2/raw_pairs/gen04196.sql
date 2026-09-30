SELECT id, full_name
FROM employees
WHERE hire_date < '2023-03-01'
UNION ALL
SELECT id, full_name
FROM employees
WHERE hire_date >= '2024-08-26';

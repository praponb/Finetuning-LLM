SELECT id, level
FROM employees
WHERE hire_date BETWEEN '2023-05-29' AND '2025-06-15'
UNION ALL
SELECT id, level
FROM employees
WHERE hire_date >= '2023-12-01'
ORDER BY id

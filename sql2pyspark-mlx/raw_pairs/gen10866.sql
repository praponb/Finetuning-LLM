SELECT id, level
FROM employees
WHERE level = 'L2'
INTERSECT
SELECT id, level
FROM employees
WHERE hire_date > '2024-11-01'

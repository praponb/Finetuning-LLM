SELECT id, hire_date
FROM employees
WHERE hire_date < '2025-07-01'
UNION ALL
SELECT id, hire_date
FROM employees
WHERE manager_id IS NULL
ORDER BY hire_date

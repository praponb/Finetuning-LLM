SELECT email, manager_id
FROM employees
WHERE hire_date >= '2023-09-01'
UNION
SELECT email, manager_id
FROM employees
WHERE dept_id IN (30, 36)
ORDER BY email

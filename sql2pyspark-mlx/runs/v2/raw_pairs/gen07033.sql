SELECT id, email, salary
FROM employees
WHERE dept_id IN (1, 22, 37) AND full_name NOT LIKE '%Chen%'
ORDER BY salary DESC

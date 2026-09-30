SELECT e.id, e.salary
FROM employees e
WHERE e.manager_id IS NULL AND e.dept_id NOT IN (SELECT id FROM departments WHERE budget >= 1573000)

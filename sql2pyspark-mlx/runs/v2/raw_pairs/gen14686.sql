SELECT e.id, e.manager_id
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'HR')

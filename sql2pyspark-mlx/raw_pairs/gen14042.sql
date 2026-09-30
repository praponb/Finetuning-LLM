SELECT e.id, e.email
FROM employees e
WHERE e.salary >= 130000 AND e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Sales');

SELECT e.id, e.level
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'HR')
ORDER BY e.level DESC, e.id DESC;

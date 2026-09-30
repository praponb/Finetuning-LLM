SELECT e.id, e.level, e.email
FROM employees e
WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name IS NULL)
ORDER BY e.level

SELECT e.id, e.level, e.dept_id
FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE budget > 2249000)
ORDER BY e.level, dept_id DESC

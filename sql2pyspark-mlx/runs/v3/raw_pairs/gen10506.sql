SELECT d.id, d.budget, d.dept_name
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees)
ORDER BY dept_name, 1

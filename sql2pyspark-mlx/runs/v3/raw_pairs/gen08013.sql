SELECT d.id, d.dept_name
FROM departments d
WHERE d.budget IS NULL AND d.id IN (SELECT dept_id FROM employees)

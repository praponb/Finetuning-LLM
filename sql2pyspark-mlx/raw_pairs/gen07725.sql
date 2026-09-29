SELECT d.id, d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees WHERE manager_id = 27)

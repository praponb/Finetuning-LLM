SELECT d.id, d.location, d.budget
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees);

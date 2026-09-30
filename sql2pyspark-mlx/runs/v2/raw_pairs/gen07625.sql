SELECT d.id, d.budget
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date >= '2025-06-01')

SELECT d.id, d.dept_name, d.budget, d.location FROM departments d WHERE d.budget < 2951000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY 2

SELECT d.id, d.budget, d.dept_name, d.location FROM departments d WHERE d.budget >= 3206000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);

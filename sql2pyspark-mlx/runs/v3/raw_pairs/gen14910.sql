SELECT d.id, d.budget, d.location FROM departments d WHERE d.location = 'Berlin' AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);

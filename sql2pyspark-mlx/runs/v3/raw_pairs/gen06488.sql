SELECT d.id, d.budget, d.location FROM departments d WHERE d.location = 'New York' AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.manager_id IS NOT NULL)

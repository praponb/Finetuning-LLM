SELECT d.id, d.dept_name, d.budget FROM departments d WHERE d.budget BETWEEN 1788000 AND 3158000 AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);

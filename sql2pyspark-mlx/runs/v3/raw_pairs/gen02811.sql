SELECT d.id, d.dept_name, d.location FROM departments d WHERE d.dept_name IN ('Marketing', 'HR', 'Legal') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.id;

SELECT d.id, d.dept_name, d.budget FROM departments d WHERE d.dept_name IN ('Engineering', 'Sales', 'Legal') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY budget DESC;

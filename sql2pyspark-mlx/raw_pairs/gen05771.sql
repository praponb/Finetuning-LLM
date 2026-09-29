SELECT d.id, d.location, d.budget FROM departments d WHERE d.dept_name IN ('Legal', 'HR', 'Engineering') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY location;

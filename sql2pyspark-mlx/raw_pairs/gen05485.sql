SELECT d.id, d.dept_name, d.location, d.budget FROM departments d WHERE d.location IN ('New York', 'Berlin', 'Tokyo') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);

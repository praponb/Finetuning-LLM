SELECT d.id, d.dept_name FROM departments d WHERE d.location IN ('Chiang Mai', 'London', 'New York') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY dept_name

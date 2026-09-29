SELECT d.location, e.is_remote FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.hire_date < '2024-03-06'

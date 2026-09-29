SELECT d.location, COUNT(e.id) AS employee_count FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.location;

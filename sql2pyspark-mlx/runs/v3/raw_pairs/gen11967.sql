SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count FROM departments d INNER JOIN employees e ON e.dept_id = d.id WHERE d.dept_name != 'Finance' GROUP BY d.id, d.dept_name;

SELECT d.id, d.dept_name, COUNT(e.id) employee_count FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id GROUP BY d.id, d.dept_name ORDER BY employee_count, id, dept_name LIMIT 1

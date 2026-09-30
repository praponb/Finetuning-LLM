SELECT d.location, d.dept_name, e.email, e.dept_id, e.hire_date FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.hire_date <= '2024-02-01' ORDER BY email

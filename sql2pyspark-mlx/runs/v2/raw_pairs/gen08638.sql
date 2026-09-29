SELECT e.is_remote, d.dept_name, d.location, d.budget FROM employees e JOIN departments d ON d.id = e.dept_id WHERE d.dept_name = 'Finance' ORDER BY dept_name;

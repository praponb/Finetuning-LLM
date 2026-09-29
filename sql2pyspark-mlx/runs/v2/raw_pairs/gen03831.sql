SELECT d.budget, d.location, d.dept_name, e.dept_id, e.manager_id FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE d.dept_name IN ('Marketing', 'Sales');

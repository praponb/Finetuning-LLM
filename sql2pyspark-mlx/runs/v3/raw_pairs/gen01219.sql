SELECT d.location, d.budget, e.level, e.email, e.dept_id FROM departments d INNER JOIN employees e ON d.id = e.dept_id WHERE d.dept_name IN ('Legal', 'Sales', 'HR');

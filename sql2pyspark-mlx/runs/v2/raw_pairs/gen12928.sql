SELECT d.location, e.dept_id, e.email, e.is_remote FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE d.location IN ('Singapore', 'Bangkok');

SELECT d.location, e.hire_date, e.is_remote FROM departments d INNER JOIN employees e ON d.id = e.dept_id WHERE d.dept_name IN ('Legal', 'HR') AND e.email IS NOT NULL;

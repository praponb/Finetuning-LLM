SELECT d.location, d.budget, e.dept_id, TRIM(e.full_name) AS clean_full_name FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.full_name IS NOT NULL;

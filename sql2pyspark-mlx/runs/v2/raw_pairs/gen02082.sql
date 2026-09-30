SELECT id, location, dept_name, budget, COALESCE(dept_name, 'N/A') AS dept_name_filled FROM departments ORDER BY id DESC, location

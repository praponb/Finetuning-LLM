SELECT id, budget, location, dept_name, CONCAT(dept_name, ', ', location) AS dept_name_location FROM departments WHERE location = 'Singapore';

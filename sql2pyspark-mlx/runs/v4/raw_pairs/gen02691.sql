SELECT id, dept_name, COUNT(*) OVER (PARTITION BY dept_name) dept_name_count FROM departments WHERE location IN ('Bangkok', 'Singapore', 'Tokyo') ORDER BY dept_name ASC

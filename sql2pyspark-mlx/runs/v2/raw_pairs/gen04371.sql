SELECT id, dept_name, SUBSTRING(dept_name, 1, 4) AS dept_name_prefix, COALESCE(location, 'N/A') AS location_filled FROM departments WHERE location = 'Chiang Mai';

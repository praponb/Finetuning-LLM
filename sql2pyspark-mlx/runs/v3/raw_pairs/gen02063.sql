SELECT id, dept_name, budget, LENGTH(location) AS location_length, LOWER(dept_name) AS dept_name_lower FROM departments WHERE budget >= 1983000 ORDER BY budget DESC, id LIMIT 5

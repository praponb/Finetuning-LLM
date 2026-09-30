SELECT id, dept_name, location FROM departments WHERE location = 'New York' UNION ALL SELECT id, dept_name, location FROM departments WHERE location NOT IN ('Berlin', 'Singapore')

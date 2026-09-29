select id, location, dept_name, budget, substring(dept_name, 1, 2) as dept_name_prefix from departments where location = 'New York'

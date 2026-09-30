select id, budget, location, concat(dept_name, ', ', location) as dept_name_location from departments where location in ('Tokyo', 'Singapore');

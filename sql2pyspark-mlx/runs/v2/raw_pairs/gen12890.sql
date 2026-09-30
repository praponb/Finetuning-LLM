SELECT id, dept_name, location, TRIM(location) AS clean_location FROM departments WHERE budget BETWEEN 2845000 AND 3664000

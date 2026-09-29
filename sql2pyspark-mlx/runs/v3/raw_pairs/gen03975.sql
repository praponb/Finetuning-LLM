SELECT id, budget, dept_name, TRIM(location) AS clean_location FROM departments WHERE location <> 'New York' AND location = 'Bangkok' AND budget >= 1655000;

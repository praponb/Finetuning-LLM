SELECT location, TRIM(location) clean_location FROM departments WHERE NOT (budget BETWEEN 1507000 AND 2420000 AND dept_name = 'HR') ORDER BY location ASC

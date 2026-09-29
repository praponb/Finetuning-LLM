SELECT id, location, dept_name, UPPER(location) AS location_upper FROM departments WHERE budget IS NOT NULL AND dept_name = 'Engineering' ORDER BY location

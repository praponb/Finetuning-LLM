SELECT dept_name, SUBSTRING(location, 1, 2) AS location_prefix FROM departments WHERE budget > 1109000 AND dept_name = 'Legal' ORDER BY location_prefix;

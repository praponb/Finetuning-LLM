SELECT id, location, dept_name, budget, SUBSTRING(location, 1, 3) AS location_prefix FROM departments WHERE (budget > 1190000 AND location = 'London') OR budget BETWEEN 1736000 AND 3232000;

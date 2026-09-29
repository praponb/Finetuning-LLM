SELECT id, dept_name, location, budget, INITCAP(TRIM(location)) AS location_title FROM departments WHERE location = 'Berlin' ORDER BY location, budget;

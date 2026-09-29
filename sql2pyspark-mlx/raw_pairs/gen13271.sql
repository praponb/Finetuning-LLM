SELECT id, location, INITCAP(TRIM(location)) AS location_title FROM departments WHERE dept_name IS NULL AND dept_name = 'Marketing';

SELECT id, dept_name, location, budget, COALESCE(location, 'Unknown') AS location_filled, UPPER(location) AS location_upper FROM departments WHERE dept_name IS NOT NULL ORDER BY budget DESC;

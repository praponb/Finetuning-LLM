SELECT id, dept_name, budget, location, COALESCE(location, 'unassigned') AS location_filled FROM departments WHERE dept_name = 'Marketing' AND location = 'New York'

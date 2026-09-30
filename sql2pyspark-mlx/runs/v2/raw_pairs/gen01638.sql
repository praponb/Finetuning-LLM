SELECT id, budget, CONCAT_WS(',', dept_name, location) AS dept_name_location_key, COALESCE(dept_name, 'unassigned') AS dept_name_filled FROM departments

SELECT id, location, COALESCE(dept_name, 'Unknown') AS dept_name_filled, COALESCE(budget, 0) AS budget_filled FROM departments

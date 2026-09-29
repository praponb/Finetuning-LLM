SELECT id, location, LOWER(location) location_lower, ROUND(budget, 1) budget_rounded FROM departments WHERE dept_name = 'Finance' ORDER BY id DESC

SELECT id, budget, location, dept_name, CEIL(budget) AS budget_whole, CONCAT(dept_name, ' - ', location) AS dept_name_location FROM departments

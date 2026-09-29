SELECT dept_name, LOWER(dept_name) AS dept_name_lower, COALESCE(budget, 0) AS budget_filled
FROM departments

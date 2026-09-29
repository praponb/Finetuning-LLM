SELECT id, dept_name, location, budget, ROUND(budget, 2) AS budget_rounded, LOWER(dept_name) AS dept_name_lower FROM departments WHERE budget >= 3980000 ORDER BY budget, location DESC

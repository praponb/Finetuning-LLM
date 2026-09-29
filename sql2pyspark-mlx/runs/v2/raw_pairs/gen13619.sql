SELECT id, dept_name, location, budget, ROUND(budget, 0) AS budget_rounded FROM departments WHERE location = 'Berlin' AND dept_name IS NULL

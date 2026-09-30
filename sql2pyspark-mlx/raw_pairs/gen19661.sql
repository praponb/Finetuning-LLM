SELECT id, budget, location, dept_name, ROUND(budget, 1) AS budget_rounded FROM departments WHERE dept_name IN ('Sales', 'Engineering', 'HR')

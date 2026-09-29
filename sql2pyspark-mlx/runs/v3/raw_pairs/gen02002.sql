SELECT id, budget, LENGTH(location) AS location_length, ROUND(budget, 0) AS budget_rounded FROM departments WHERE dept_name != 'Marketing' AND dept_name = 'Legal' ORDER BY location_length DESC;

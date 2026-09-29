SELECT dept_name, MAX(budget) - MIN(budget) AS budget_range FROM departments WHERE location = 'Berlin' GROUP BY dept_name HAVING MAX(budget) - MIN(budget) > 32;

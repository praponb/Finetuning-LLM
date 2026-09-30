SELECT id, location, dept_name, budget, MAX(budget) OVER (PARTITION BY location, dept_name) AS location_max_budget FROM departments;

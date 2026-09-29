SELECT id, location, dept_name, budget, MIN(budget) OVER (PARTITION BY location, dept_name) AS location_min_budget FROM departments WHERE budget <= 1222000;

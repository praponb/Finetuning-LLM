SELECT id, dept_name, location, budget, LAG(budget, 1) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS prev_budget FROM departments;

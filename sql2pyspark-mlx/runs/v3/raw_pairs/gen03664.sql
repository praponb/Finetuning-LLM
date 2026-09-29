SELECT id, dept_name, budget, SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id) running_budget FROM departments WHERE dept_name != 'HR'

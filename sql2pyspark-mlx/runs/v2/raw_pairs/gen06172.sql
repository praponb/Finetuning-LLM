SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_row, budget - LAG(budget, 2) OVER (ORDER BY budget, id) AS budget_change FROM departments;

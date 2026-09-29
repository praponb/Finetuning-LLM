SELECT id, dept_name, location, budget, DENSE_RANK() OVER (PARTITION BY dept_name, location ORDER BY budget) AS budget_dense_rank FROM departments;

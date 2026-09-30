SELECT id, dept_name, budget, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_dense_rank FROM departments ORDER BY 4;

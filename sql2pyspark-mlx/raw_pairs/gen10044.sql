SELECT id, dept_name, location, budget, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_rank, ROW_NUMBER() OVER (ORDER BY budget, id) AS seq_no FROM departments;

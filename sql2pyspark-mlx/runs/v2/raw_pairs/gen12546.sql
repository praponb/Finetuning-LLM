SELECT id, budget, ROW_NUMBER() OVER (ORDER BY budget, id) AS budget_row, DENSE_RANK() OVER (ORDER BY budget DESC) AS dense_rnk FROM departments WHERE dept_name IN ('Legal', 'HR', 'Sales');

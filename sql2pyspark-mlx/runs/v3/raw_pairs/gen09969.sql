SELECT
    id,
    dept_name,
    budget,
    LEAD(budget, 2, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_dense_rank
FROM departments
ORDER BY budget_dense_rank DESC

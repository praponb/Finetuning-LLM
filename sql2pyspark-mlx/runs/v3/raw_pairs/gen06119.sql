SELECT
    id,
    location,
    budget,
    dept_name,
    DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_dense_rank,
    SUM(budget) OVER (PARTITION BY location, dept_name) AS location_sum_budget
FROM departments;

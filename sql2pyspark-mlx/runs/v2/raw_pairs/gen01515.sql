SELECT
    id,
    dept_name,
    budget,
    MIN(budget) OVER (PARTITION BY dept_name) AS dept_name_min_budget
FROM departments

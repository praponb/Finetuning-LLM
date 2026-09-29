SELECT
    id,
    dept_name,
    budget,
    MAX(budget) OVER (PARTITION BY dept_name) AS dept_name_max_budget
FROM departments;

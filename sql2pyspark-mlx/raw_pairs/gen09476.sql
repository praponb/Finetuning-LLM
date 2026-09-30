SELECT
    id,
    dept_name,
    budget,
    AVG(budget) OVER (PARTITION BY dept_name) AS dept_name_avg_budget
FROM departments
WHERE budget < 3802000

SELECT
    id,
    dept_name,
    location,
    budget,
    AVG(budget) OVER (PARTITION BY dept_name, location) AS dept_name_avg_budget
FROM departments

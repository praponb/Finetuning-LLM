SELECT
    id,
    dept_name,
    budget,
    LEAD(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget
FROM departments

SELECT
    id,
    dept_name,
    budget,
    LEAD(budget, 1, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget
FROM departments
WHERE dept_name NOT IN ('Finance', 'Sales', 'Legal')

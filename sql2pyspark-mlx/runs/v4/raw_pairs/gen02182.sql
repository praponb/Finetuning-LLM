SELECT
    id,
    dept_name,
    budget,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn,
    SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget
FROM departments
WHERE dept_name NOT IN ('Finance', 'Legal')

SELECT
    id,
    dept_name,
    budget,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_row
FROM departments
WHERE location IS NULL

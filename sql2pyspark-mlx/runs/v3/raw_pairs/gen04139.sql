SELECT
    id,
    dept_name,
    budget,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) budget_row
FROM departments

SELECT
    id,
    dept_name,
    budget,
    budget - LAG(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) budget_change
FROM departments
WHERE location = 'Tokyo';

SELECT location, budget_count, max_budget
FROM (
    SELECT location, COUNT(budget) AS budget_count, MAX(budget) AS max_budget
    FROM departments
    WHERE dept_name != 'Sales'
    GROUP BY location
) AS x
WHERE budget_count > 4

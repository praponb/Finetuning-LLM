SELECT
    dept_name,
    location,
    MAX(budget) AS max_budget,
    MAX(budget) - MIN(budget) AS budget_range
FROM departments
GROUP BY dept_name, location
ORDER BY max_budget DESC;

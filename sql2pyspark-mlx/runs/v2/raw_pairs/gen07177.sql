SELECT
    location,
    dept_name,
    MAX(budget) - MIN(budget) AS budget_range,
    ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
GROUP BY location, dept_name
HAVING MAX(budget) - MIN(budget) > 61
ORDER BY stddev_budget DESC;

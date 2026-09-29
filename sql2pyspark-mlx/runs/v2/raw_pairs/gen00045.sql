SELECT ROUND(STDDEV(budget), 2) AS stddev_budget, MIN(budget) AS min_budget
FROM departments
WHERE dept_name IN ('Marketing', 'HR', 'Sales') AND location IN ('New York', 'Bangkok')
ORDER BY min_budget DESC

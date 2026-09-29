SELECT *
FROM (
    SELECT dept_name, ROUND(STDDEV(budget), 2) AS stddev_budget, COUNT(*) AS row_count
    FROM departments
    WHERE dept_name = 'Engineering'
    GROUP BY dept_name
) AS t
WHERE row_count < 6
ORDER BY stddev_budget, dept_name
LIMIT 20

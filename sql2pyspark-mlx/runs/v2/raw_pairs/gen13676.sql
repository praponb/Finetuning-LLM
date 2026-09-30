SELECT *
FROM (
    SELECT dept_name, COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) tokyo_count, ROUND(AVG(budget), 2) avg_budget
    FROM departments
    WHERE dept_name = 'Sales'
    GROUP BY dept_name
) x
WHERE tokyo_count >= 9
ORDER BY tokyo_count DESC, avg_budget, dept_name
LIMIT 20

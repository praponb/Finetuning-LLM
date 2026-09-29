SELECT dept_name, london_budget, total_budget
FROM (
    SELECT dept_name, SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) london_budget, SUM(budget) total_budget
    FROM departments
    WHERE location IN ('Singapore', 'Tokyo')
    GROUP BY dept_name
) sub
WHERE total_budget >= 16552000
ORDER BY total_budget ASC;

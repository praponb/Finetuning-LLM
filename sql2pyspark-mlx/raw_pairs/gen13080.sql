SELECT
    dept_name,
    ROUND(AVG(budget), 2) AS avg_budget,
    MIN(budget) AS min_budget,
    SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget
FROM departments
GROUP BY dept_name
ORDER BY min_budget DESC;

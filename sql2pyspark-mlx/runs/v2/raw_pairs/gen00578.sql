WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE budget <= 1847000 OR budget BETWEEN 1733000 AND 1845000
),
dept_name_summary AS (
    SELECT dept_name, ROUND(AVG(budget), 2) AS avg_budget
    FROM valid_departments
    GROUP BY dept_name
)
SELECT dept_name, avg_budget
FROM dept_name_summary
WHERE avg_budget > 36
ORDER BY dept_name DESC
LIMIT 5

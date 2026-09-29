WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE location IN ('New York', 'Tokyo', 'Chiang Mai')
),
dept_name_summary AS (
    SELECT dept_name, SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget
    FROM recent_departments
    GROUP BY dept_name
)
SELECT *
FROM dept_name_summary
WHERE new_york_budget < 32
ORDER BY new_york_budget DESC, dept_name
LIMIT 20;

WITH src AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 1280000 AND 3784000 AND location != 'Singapore'
),
summary AS (
    SELECT location, MAX(budget) - MIN(budget) AS budget_range, AVG(budget) AS avg_budget
    FROM src
    GROUP BY location
)
SELECT *
FROM summary
ORDER BY budget_range DESC, avg_budget DESC

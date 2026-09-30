WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 1822000 AND 2229000
),
departments_stats AS (
    SELECT dept_name, SUM(budget) AS total_budget, MAX(budget) - MIN(budget) AS budget_range
    FROM valid_departments
    GROUP BY dept_name
)
SELECT *
FROM departments_stats
WHERE budget_range >= 37
ORDER BY total_budget DESC;

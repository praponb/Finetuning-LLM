WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE salary < 137000 OR salary <= 105000
),
employees_stats AS (
    SELECT level, COUNT(DISTINCT salary) AS unique_levels
    FROM valid_employees
    GROUP BY level
)
SELECT level, unique_levels
FROM employees_stats
WHERE unique_levels > 10
ORDER BY 1

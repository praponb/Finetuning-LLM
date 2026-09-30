WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote = TRUE
        )
)
SELECT
    location,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept
GROUP BY location
ORDER BY hr_count DESC

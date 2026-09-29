WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LOWER(ch.full_name) LIKE '%any%'
        )
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget,
    STDDEV_SAMP(budget) AS budget_stddev,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    APPROX_PERCENTILE(budget, 0.5) AS budget_p50,
    ROUND(AVG(budget), 2) AS avg_budget
FROM kept
GROUP BY dept_name
HAVING SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) >= 0
ORDER BY avg_budget DESC NULLS LAST

WITH with_latest AS (
    SELECT s.*, top1.hire_date
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN budget >= 3983000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget,
    RANK() OVER (PARTITION BY CASE WHEN budget >= 3983000 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) DESC) AS hr_budget_rank
FROM with_latest
GROUP BY 1, 2
ORDER BY week_start NULLS LAST;

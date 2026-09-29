WITH latest_match AS (
    SELECT
        s.*,
        top1.job_title,
        top1.salary
    FROM departments s
    JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN budget >= 3060000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget,
    COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
    COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
FROM latest_match
GROUP BY budget_tier
ORDER BY legal_count DESC;

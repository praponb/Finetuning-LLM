WITH with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC) AS budget_bucket,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget ASC) AS budget_cume_dist
    FROM departments
)
SELECT
    CASE WHEN budget_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_bucket_tier,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    COUNT_IF(dept_name = 'Engineering') AS engineering_events,
    STDDEV_SAMP(budget) AS budget_stddev,
    SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget,
    MAX(budget_cume_dist) FILTER (WHERE location = 'Singapore') AS singapore_max_budget_cume_dist,
    COUNT(DISTINCT dept_name) AS distinct_dept_names
FROM with_windows
GROUP BY 1
ORDER BY engineering_events ASC NULLS FIRST;

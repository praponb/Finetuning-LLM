WITH valid AS (
    SELECT
        *,
        CASE WHEN budget >= 4378000 THEN 'High' WHEN budget >= 2031000 THEN 'Medium' ELSE 'Low' END budget_tier,
        COALESCE(dept_name, 'OTHER') dept_name_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
base AS (
    SELECT *, COALESCE(location, 'N/A') location_filled
    FROM valid
    WHERE dept_name IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title IN ('Director', 'Analyst')
        )
),
filtered AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_filled,
        CASE WHEN dept_name IN ('Finance', 'Marketing') THEN 'Priority' ELSE 'Other' END dept_name_group,
        COALESCE(location, 'UNKNOWN') location_filled_2
    FROM eligible
    WHERE dept_name = 'HR'
)
SELECT
    CASE WHEN budget >= 4142000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    APPROX_PERCENTILE(budget, 0.5) AS budget_p50,
    COUNT_IF(dept_name = 'Legal') AS legal_events,
    SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget
FROM filtered
GROUP BY CUBE (CASE WHEN budget >= 4142000 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) >= 0
ORDER BY budget_tier;

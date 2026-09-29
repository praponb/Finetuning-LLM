WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget NULLS FIRST, id) AS latest_rank
        FROM departments
    ) t
    WHERE latest_rank = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_job_title,
        latest.hire_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title = 'Intern'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id = 31
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    dept_name,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    COUNT(*) AS event_count,
    GROUPING(DATE_TRUNC('YEAR', hire_date)) AS gid
FROM kept
GROUP BY ROLLUP (DATE_TRUNC('YEAR', hire_date), dept_name)
ORDER BY year_start;

WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(location) AS location_length
    FROM departments
    WHERE dept_name = 'Legal'
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_job_title
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY budget DESC NULLS LAST, id) AS latest_rank
        FROM with_last
    ) t
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN location_length >= 68 THEN 'LARGE' ELSE 'SMALL' END AS location_length_tier,
    dept_name,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    GROUPING(CASE WHEN location_length >= 68 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN location_length >= 68 THEN 'LARGE' ELSE 'SMALL' END, dept_name), (CASE WHEN location_length >= 68 THEN 'LARGE' ELSE 'SMALL' END))
HAVING chiang_mai_count < 1

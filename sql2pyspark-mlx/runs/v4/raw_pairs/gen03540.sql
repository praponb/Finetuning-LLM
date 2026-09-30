WITH cleaned AS (
    SELECT
        *,
        CASE WHEN location = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE dept_name NOT IN ('Engineering', 'Legal', 'Sales')
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER w_running AS budget_dense_rank,
        STDDEV_SAMP(is_bangkok) OVER w_hist AS location_stddev_samp_is_bangkok,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket
    FROM cleaned
    WINDOW w_running AS (PARTITION BY location ORDER BY budget NULLS LAST), w_hist AS (PARTITION BY location, dept_name)
),
with_latest AS (
    SELECT s.*, x.employee_job_title
    FROM enriched s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    SELECT id, dept_name
    FROM with_latest
    INTERSECT ALL
    SELECT id, dept_name
    FROM departments
    WHERE budget >= 2284000
),
kept_rows AS (
    SELECT s.*
    FROM with_latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
metrics AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        is_bangkok,
        dept_name_title,
        budget_dense_rank,
        location_stddev_samp_is_bangkok,
        budget_bucket,
        employee_job_title,
        DENSE_RANK() OVER (PARTITION BY employee_job_title ORDER BY is_bangkok NULLS FIRST) AS is_bangkok_dense_rank,
        STDDEV_SAMP(budget_dense_rank) OVER (PARTITION BY employee_job_title) AS employee_job_title_stddev_samp_budget_dense_rank,
        ROUND(budget_dense_rank / SUM(budget_dense_rank) OVER (PARTITION BY dept_name), 4) AS budget_dense_rank_share,
        COUNT(is_bangkok) OVER (PARTITION BY employee_job_title, dept_name) AS employee_job_title_count_is_bangkok,
        PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY location_stddev_samp_is_bangkok DESC NULLS LAST) AS location_stddev_samp_is_bangkok_percent_rank
    FROM kept
),
totals AS (
    SELECT
        employee_job_title,
        COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows,
        SUM(budget) AS total_budget,
        ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END), 0), 2) AS budget_per_marketing,
        GROUPING_ID() AS grouping_id
    FROM metrics
    GROUP BY ROLLUP (employee_job_title)
    HAVING total_budget < 0
)
SELECT
    employee_job_title,
    legal_rows,
    budget_per_marketing,
    grouping_id,
    CASE WHEN employee_job_title RLIKE '^(Director|Engineer)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS employee_job_title_zone
FROM totals
ORDER BY grouping_id NULLS FIRST

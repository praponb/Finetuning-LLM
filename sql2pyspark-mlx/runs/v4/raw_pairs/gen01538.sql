WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN location IN ('Singapore', 'Chiang Mai') THEN 'Core' ELSE 'Other' END AS location_group,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE location IN ('London', 'Berlin', 'Singapore')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.job_title
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_group,
        dept_name_clean,
        latest_rank,
        job_title,
        SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget,
        CUME_DIST() OVER (PARTITION BY location, job_title ORDER BY budget NULLS LAST) AS budget_cume_dist,
        AVG(budget) OVER (PARTITION BY job_title, location ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5,
        SUM(budget) OVER (PARTITION BY dept_name, job_title ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget
    FROM latest_match
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget_avg_5, 0) AS budget_avg_5_rounded,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM with_windows
    WHERE dept_name NOT IN ('Marketing', 'HR', 'Engineering')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary <= 101000
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_avg_5 NULLS LAST, id) AS rn
        FROM not_flagged
    ) ranked
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    location,
    job_title,
    STDDEV_SAMP(budget) AS budget_stddev,
    APPROX_PERCENTILE(budget, 0.5) AS budget_p50,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    ROUND(SUM(cumulative_budget) / NULLIF(COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END), 0), 2) AS cumulative_budget_per_engineer,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    AVG(budget_avg_5_rounded) AS avg_budget_avg_5_rounded
FROM latest
GROUP BY location, job_title
HAVING ROUND(SUM(cumulative_budget) / NULLIF(COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END), 0), 2) < 0

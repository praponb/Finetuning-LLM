WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN dept_name IN ('Engineering', 'Sales') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE dept_name NOT IN ('Sales', 'HR', 'Legal')
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.salary,
        top1.job_title
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY budget NULLS LAST, id) AS latest_rank
        FROM with_last
    ) x
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    dept_name,
    location,
    STDDEV_SAMP(budget) AS budget_stddev,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles
FROM latest
GROUP BY dept_name, location
ORDER BY location DESC

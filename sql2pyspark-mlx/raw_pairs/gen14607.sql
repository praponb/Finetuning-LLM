WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN job_title IN ('Director', 'Engineer') THEN 'Tier 1' ELSE 'Other' END AS job_title_group,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-03-04'
      AND email LIKE '%@GMAIL.COM'
),
joined AS (
    -- Attach employees attributes
    SELECT s.*, d.salary AS employee_salary
    FROM base s
    INNER JOIN employees d
        ON s.manager_id <=> d.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level, job_title
    FROM joined
    INTERSECT
    SELECT level, job_title
    FROM employees
    WHERE level IN ('L5', 'L1')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.level = k.level AND s.job_title = k.job_title
),
enriched AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.manager_id,
        s.salary,
        s.hire_date,
        s.is_remote,
        s.resolved_job_title,
        s.employee_salary,
        b.email AS employee_email
    FROM kept_rows s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        employee_salary,
        employee_email,
        ROUND(salary / SUM(salary) OVER (PARTITION BY YEAR(hire_date), manager_id), 4) AS salary_share,
        PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY employee_salary DESC) AS employee_salary_percent_rank,
        STDDEV_SAMP(salary) OVER (PARTITION BY manager_id) AS manager_id_stddev_samp_salary
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_share_tier,
    CASE WHEN employee_salary_percent_rank >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS employee_salary_percent_rank_tier,
    PERCENTILE_APPROX(employee_salary_percent_rank, 0.9) AS employee_salary_percent_rank_p90,
    APPROX_PERCENTILE(employee_salary_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS employee_salary_percent_rank_quartiles
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN salary_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN employee_salary_percent_rank >= 0.78 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN salary_share >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY salary_share_tier NULLS FIRST;

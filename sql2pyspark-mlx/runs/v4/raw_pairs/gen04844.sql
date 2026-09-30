WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(email, 'unassigned') AS email_filled,
        CASE WHEN salary >= 125000 THEN 'High' WHEN salary >= 64000 THEN 'Medium' ELSE 'Low' END AS salary_band,
        COALESCE(level, 'DIRECT') AS level_clean
    FROM employees
    WHERE hire_date >= DATE '2024-01-07'
      AND level IS NOT NULL
      AND LOWER(TRIM(full_name)) = 'olivia garcia'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        email_filled,
        salary_band,
        level_clean,
        LAG(salary, 2) OVER (ORDER BY hire_date, id) AS prev_salary,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        PERCENT_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_percent_rank,
        NTILE(5) OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC, id) AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM base
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.job_title AS employee_job_title,
        p.level AS employee_level,
        p.hire_date AS employee_hire_date
    FROM metrics a
    LEFT JOIN employees p
        ON a.manager_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY employee_hire_date DESC, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        email_filled,
        salary_band,
        level_clean,
        prev_salary,
        last_known_level,
        salary_percent_rank,
        salary_bucket,
        salary_share,
        employee_job_title,
        employee_level,
        employee_hire_date,
        latest_rank,
        SUM(salary_share) OVER (PARTITION BY employee_level ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_share,
        AVG(salary_bucket) OVER (PARTITION BY employee_level) AS employee_level_avg_salary_bucket,
        SUM(salary_percent_rank) OVER w_recent AS cumulative_salary_percent_rank,
        NTILE(5) OVER (PARTITION BY level ORDER BY salary_bucket, id) AS salary_bucket_bucket
    FROM ranked
    WINDOW w_recent AS (PARTITION BY manager_id, dept_id ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS avg_employee_salary
FROM with_windows s;

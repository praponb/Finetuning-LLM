WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(full_name, 1, 2) AS full_name_prefix,
        COALESCE(level, 'OTHER') AS level_filled
    FROM employees
    WHERE job_title IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.*,
        d.email AS employee_email,
        d.salary AS employee_salary
    FROM base a
    JOIN employees d
        ON a.manager_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(employee_email) AS employee_email_lower,
        CEIL(salary) AS salary_whole,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM joined
    WHERE employee_salary > 73000
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
        full_name_prefix,
        level_filled,
        employee_email,
        employee_salary,
        employee_email_lower,
        salary_whole,
        job_title_clean,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY salary_whole, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3,
        NTILE(10) OVER (PARTITION BY dept_id, manager_id ORDER BY salary, id) AS salary_bucket,
        NTILE(3) OVER (PARTITION BY dept_id ORDER BY salary, id) AS salary_bucket_2
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary_whole >= 76 THEN 'LARGE' ELSE 'SMALL' END AS salary_whole_tier,
    APPROX_PERCENTILE(salary, 0.95) AS salary_p95,
    ROUND(AVG(salary_bucket_2), 2) AS avg_salary_bucket_2,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(DISTINCT level) AS distinct_levels
FROM metrics
GROUP BY salary_whole_tier
HAVING COUNT(DISTINCT level) >= 1
ORDER BY salary_p95

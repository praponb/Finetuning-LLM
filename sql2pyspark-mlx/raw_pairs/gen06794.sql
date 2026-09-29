WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        COALESCE(level, 'Unknown') level_filled
    FROM employees
    WHERE hire_date >= DATE '2023-06-22'
      AND level IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY YEAR(hire_date), level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_level,
        NTILE(10) OVER w_recent salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, job_title), 4) salary_share,
        SUM(salary) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_salary
    FROM base
    WINDOW w_recent AS (PARTITION BY job_title, manager_id ORDER BY salary ASC, id ASC)
),
base_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        level_filled,
        last_known_level,
        salary_share,
        running_salary,
        DATE_TRUNC('month', hire_date) hire_date_period
    FROM with_windows
    WHERE hire_date >= TO_DATE('2023-09-12')
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.full_name,
        e.manager_id,
        e.job_title,
        e.salary,
        e.is_remote,
        e.level,
        e.level_filled,
        e.last_known_level,
        e.salary_share,
        e.hire_date_period,
        p2.email AS employee_email
    FROM base_employees e
    INNER JOIN employees p2
        ON e.manager_id = p2.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        manager_id,
        salary,
        is_remote,
        level,
        level_filled,
        salary_share,
        LEAD(salary_share, 1) OVER (PARTITION BY DATE_TRUNC('week', hire_date_period), manager_id ORDER BY salary ASC, id ASC) next_salary_share,
        AVG(salary) OVER (ORDER BY salary_share ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) salary_avg_5_prior
    FROM enriched
    WHERE level <> 'L2'
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.full_name,
        a.manager_id,
        a.level,
        a.salary_share,
        e.job_title,
        e.level AS employee_level
    FROM metrics a
    LEFT JOIN employees e
        ON a.manager_id = e.id
)
SELECT
    id,
    full_name,
    level,
    job_title,
    employee_level
FROM with_dims
ORDER BY job_title ASC

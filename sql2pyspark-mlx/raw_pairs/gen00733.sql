WITH valid AS (
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
        DATE_TRUNC('year', hire_date) AS hire_date_period,
        COALESCE(level, 'NONE') AS level_filled
    FROM employees
    WHERE manager_id IS NOT NULL
)
SELECT
    id,
    full_name,
    email,
    salary,
    is_remote,
    level,
    hire_date_period,
    NTILE(10) OVER (PARTITION BY level, dept_id ORDER BY salary DESC, id) AS salary_bucket,
    RANK() OVER (ORDER BY salary DESC) AS salary_rank,
    RANK() OVER (PARTITION BY job_title, dept_id ORDER BY salary) AS salary_rank_2
FROM valid;

WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, IF(is_remote, 'Yes', 'No') AS is_remote_label
    FROM employees
    WHERE hire_date >= TO_DATE('2023-07-03')
      AND manager_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary, 2, 0) OVER (PARTITION BY DATE_TRUNC('month', hire_date), job_title ORDER BY salary, id) AS next_salary,
        LAG(salary, 2, 0) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary
    FROM base
    WHERE job_title IN ('Manager', 'Director')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        next_salary * salary AS next_salary_times_salary,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM with_windows
    WHERE hire_date >= TO_DATE('2023-06-09')
      AND manager_id IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.email,
        e.manager_id,
        e.job_title,
        e.salary,
        e.is_remote,
        e.is_remote_label,
        e.next_salary,
        e.next_salary_times_salary,
        p.job_title AS employee_job_title
    FROM filtered e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        job_title,
        salary,
        is_remote,
        employee_job_title,
        LENGTH(job_title) AS job_title_length,
        COALESCE(employee_job_title, 'N/A') AS resolved_employee_job_title
    FROM joined
    WHERE is_remote
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY job_title_length DESC, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    job_title,
    salary,
    is_remote,
    job_title_length,
    CASE WHEN job_title RLIKE '^(Manager|Designer)$' THEN 'CORE' ELSE 'PRIORITY' END AS job_title_zone
FROM latest
ORDER BY job_title DESC NULLS LAST, job_title_length;

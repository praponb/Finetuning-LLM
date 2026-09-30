WITH with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        PERCENT_RANK() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS LAST) AS budget_percent_rank
    FROM departments
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        latest.hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.hire_date,
        x.job_title,
        x.employee_salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    SELECT id
    FROM with_latest
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE budget >= 2789000
),
kept_rows AS (
    SELECT s.*
    FROM with_latest s
    INNER JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    SELECT *, INITCAP(TRIM(job_title)) AS job_title_title
    FROM kept_rows
    WHERE job_title IS NOT NULL
)
SELECT
    id,
    hire_date,
    job_title,
    employee_salary,
    job_title_title
FROM filtered
ORDER BY employee_salary

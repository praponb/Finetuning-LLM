WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        ROUND(salary * 0.9, 2) AS adjusted_salary
    FROM employees
    WHERE manager_id = 39
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        d.full_name AS employee_full_name,
        d.level AS employee_level,
        d.email AS employee_email
    FROM cleaned e
    INNER JOIN employees d
        ON e.manager_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        adjusted_salary,
        employee_full_name,
        employee_level,
        employee_email,
        ROUND(salary / SUM(salary) OVER (PARTITION BY employee_level, manager_id), 4) AS salary_share,
        LAG(salary, 1) OVER (PARTITION BY dept_id, employee_level ORDER BY adjusted_salary, id) AS prev_salary
    FROM with_dims
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY employee_level, dept_id ORDER BY salary_share DESC) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    full_name,
    email,
    salary,
    is_remote,
    level,
    employee_full_name,
    employee_level,
    employee_email
FROM latest
ORDER BY salary NULLS FIRST

WITH filtered AS (
    SELECT
        id,
        manager_id,
        job_title,
        is_remote,
        level,
        CONCAT(job_title, ' - ', email) job_title_email
    FROM employees
    WHERE job_title IS NOT NULL
),
joined AS (
    SELECT
        src.id,
        src.manager_id,
        src.job_title,
        src.job_title_email,
        e.full_name,
        e.level AS employee_level,
        e.job_title AS employee_job_title
    FROM filtered src
    LEFT JOIN employees e
        ON src.manager_id = e.id
    WHERE e.job_title NOT IN ('Manager', 'Analyst')
),
with_dims AS (
    SELECT
        src.id,
        src.manager_id,
        src.job_title,
        src.full_name,
        src.employee_level,
        d.email,
        d.level
    FROM joined src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
kept_keys AS (
    SELECT manager_id, id
    FROM with_dims
    INTERSECT
    SELECT manager_id, id
    FROM employees
    WHERE hire_date BETWEEN '2024-03-01' AND '2024-09-01'
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
joined_employees AS (
    SELECT
        s.*,
        e.full_name AS employee_full_name,
        e.hire_date,
        e.email AS employee_email
    FROM kept_rows s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
joined_employees_2 AS (
    SELECT
        e.manager_id,
        e.job_title,
        e.full_name,
        e.employee_level,
        e.level,
        e.employee_full_name,
        e.hire_date,
        e.employee_email,
        p.level AS employee_level_2
    FROM joined_employees e
    INNER JOIN employees p
        ON e.manager_id <=> p.id
)
SELECT
    manager_id,
    job_title,
    full_name,
    employee_level,
    level,
    employee_full_name,
    hire_date,
    employee_email
FROM joined_employees_2
ORDER BY level DESC NULLS LAST, job_title;

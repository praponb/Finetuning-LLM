WITH valid AS (
    SELECT
        *,
        ROUND(salary, 0) salary_rounded,
        COALESCE(job_title, 'OTHER') resolved_job_title
    FROM employees
    WHERE job_title IS NOT NULL
),
latest AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date ASC NULLS FIRST, id ASC) = 1
),
ranked AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id ASC) = 1
),
with_dims AS (
    SELECT
        s.*,
        b.email AS employee_email,
        b.salary AS employee_salary
    FROM ranked s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
cleaned AS (
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
        resolved_job_title,
        employee_email,
        FLOOR(salary) salary_whole
    FROM with_dims
    WHERE hire_date > DATE '2023-11-28'
      AND manager_id IS NOT NULL
),
kept_keys AS (
    SELECT id, dept_id
    FROM cleaned
    EXCEPT ALL
    SELECT id, dept_id
    FROM employees
    WHERE salary BETWEEN 133000 AND 140000
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.id = k.id AND s.dept_id = k.dept_id
),
base AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        hire_date,
        is_remote,
        level,
        resolved_job_title,
        employee_email,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label,
        COALESCE(level, 'UNKNOWN') resolved_level
    FROM verified
    WHERE manager_id IS NOT NULL
)
SELECT
    id,
    full_name,
    dept_id,
    is_remote,
    level,
    resolved_job_title,
    employee_email,
    is_remote_label,
    resolved_level
FROM base
ORDER BY id DESC NULLS FIRST;

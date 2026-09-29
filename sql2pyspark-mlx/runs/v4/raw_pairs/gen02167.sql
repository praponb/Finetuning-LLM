WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        is_remote,
        level,
        DATE_ADD(hire_date, 70) hire_date_plus_70d,
        COALESCE(job_title, 'Unknown') job_title_filled,
        COALESCE(job_title, 'DIRECT') job_title_filled_2
    FROM employees
    WHERE level IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date_plus_70d DESC) AS rn
    FROM base
    QUALIFY rn = 1
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.*,
        b.salary,
        b.job_title AS employee_job_title
    FROM deduped src
    INNER JOIN employees b
        ON src.manager_id = b.id
),
joined_employees AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.full_name AS employee_full_name,
        p.salary AS employee_salary,
        p.hire_date
    FROM joined a
    LEFT JOIN employees p
        ON a.manager_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        employee_job_title,
        salary,
        employee_salary
    FROM joined_employees
) src
UNPIVOT (
    value FOR metric IN (salary, employee_salary)
)

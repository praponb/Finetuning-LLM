WITH valid AS (
    SELECT
        *,
        LOWER(job_title) AS job_title_lower,
        DATE_SUB(hire_date, 65) AS hire_date_minus_65d
    FROM employees
    WHERE hire_date > TO_DATE('2023-08-03')
      AND job_title IS NOT NULL
),
with_dims AS (
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.manager_id,
        src.salary,
        src.hire_date,
        src.level,
        src.hire_date_minus_65d,
        e.full_name AS employee_full_name,
        e.hire_date AS employee_hire_date
    FROM valid src
    LEFT JOIN employees e
        ON src.manager_id = e.id
),
joined AS (
    SELECT
        src.*,
        p.email AS employee_email,
        p.salary AS employee_salary,
        p.hire_date AS employee_hire_date_2
    FROM with_dims src
    LEFT JOIN employees p
        ON src.manager_id = p.id
    WHERE p.salary BETWEEN 61000 AND 78000
),
ranked AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY employee_salary DESC NULLS LAST, id) = 1
),
with_dims_employees AS (
    SELECT s.*, e.job_title
    FROM ranked s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
wide AS (
    SELECT *
    FROM (
        SELECT
            manager_id,
            level,
            employee_salary
        FROM with_dims_employees
    ) src
    PIVOT (
        SUM(employee_salary)
        FOR level IN ('L1', 'L3', 'L5')
    )
)
SELECT
    s.manager_id,
    s.L1,
    s.L5,
    (SELECT MIN(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS min_employee_salary
FROM wide s

WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
    FROM employees
    WHERE NOT is_remote
),
enriched AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.salary AS employee_salary,
        p.hire_date AS employee_hire_date,
        p.full_name AS employee_full_name
    FROM cleaned src
    JOIN employees p
        ON src.manager_id = p.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        level,
        employee_salary,
        employee_hire_date,
        employee_full_name,
        ROUND(employee_salary / salary, 2) AS employee_salary_per_salary
    FROM enriched
    WHERE salary BETWEEN 126000 AND 138000
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(employee_salary) OVER w_ordered AS manager_id_sum_employee_salary,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY employee_salary_per_salary, id) AS seq_no
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY manager_id)
)
-- Join a dimension and aggregate
SELECT
    e.job_title,
    src.level AS employee_level,
    COUNT(*) AS cnt,
    MAX(src.seq_no) AS max_seq_no,
    GROUPING_ID() AS level_id
FROM scored src
LEFT JOIN employees e ON src.manager_id = e.id
GROUP BY GROUPING SETS ((e.job_title, src.level), (e.job_title))

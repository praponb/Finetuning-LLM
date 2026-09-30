WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        CUME_DIST() OVER (PARTITION BY dept_id, manager_id ORDER BY salary DESC) AS salary_cume_dist
    FROM employees
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SPLIT(LOWER(email), '@')[0] AS email_user
    FROM metrics
    WHERE manager_id IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.email AS employee_email,
        e.hire_date AS employee_hire_date,
        e.level AS employee_level
    FROM valid s
    INNER JOIN employees e
        ON s.manager_id = e.id
    WHERE e.dept_id IN (15, 21, 35)
),
enriched AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.email AS employee_email_2,
        p.salary AS employee_salary,
        p.level AS employee_level_2
    FROM with_dims src
    LEFT JOIN employees p
        ON src.manager_id <=> p.id
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.manager_id,
        s.salary,
        s.hire_date,
        s.running_salary,
        s.email_user,
        s.employee_hire_date,
        s.employee_level,
        s.employee_email_2,
        s.employee_salary,
        s.employee_level_2,
        e.email AS employee_email_3
    FROM enriched s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
enriched_employees_2 AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.level,
        d.email AS employee_email,
        d.full_name AS employee_full_name
    FROM enriched_employees s
    INNER JOIN employees d
        ON s.manager_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        email_user,
        employee_hire_date,
        employee_level,
        employee_email_2,
        employee_salary,
        employee_email_3
    FROM enriched_employees_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id) = 1
)
-- Join a dimension and aggregate
SELECT
    dim.dept_name,
    s.employee_level,
    COUNT(*) AS cnt,
    SUM(s.salary) AS total_salary
FROM deduped s
INNER JOIN departments dim ON s.dept_id = dim.id
GROUP BY dim.dept_name, s.employee_level
ORDER BY cnt NULLS FIRST

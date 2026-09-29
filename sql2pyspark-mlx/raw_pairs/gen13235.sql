WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN budget >= 3788000 THEN 'High' WHEN budget >= 2052000 THEN 'Medium' ELSE 'Low' END budget_band
    FROM departments
    WHERE location IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS row_num
    FROM base
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.row_num,
        x.employee_level
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.row_num,
        s.employee_level,
        top1.salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.row_num,
        s.salary,
        top1.employee_salary
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT id
FROM latest_match_departments
ORDER BY id NULLS FIRST
LIMIT 5

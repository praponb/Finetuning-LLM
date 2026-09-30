WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary > 66000
        )
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.employee_level,
        latest.employee_level_2
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.level AS employee_level_2
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id = 1
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(employee_level, 'DIRECT') AS employee_level_filled
    FROM eligible
    WHERE dept_name IS NOT NULL
      AND employee_level IN ('L3', 'L4')
)
SELECT
    id,
    dept_name,
    employee_level,
    dept_name_lower
FROM base

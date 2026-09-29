WITH base AS (
    SELECT *
    FROM employees
    WHERE manager_id IS NOT NULL AND salary >= 95000
),
is_remote_summary AS (
    SELECT is_remote, MIN(hire_date) AS first_hire_date
    FROM base
    GROUP BY is_remote
)
SELECT is_remote, first_hire_date
FROM is_remote_summary;

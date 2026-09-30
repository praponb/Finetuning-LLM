WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.email LIKE '%@GMAIL.COM'
        )
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.salary,
        top1.employee_hire_date
    FROM kept s
    JOIN LATERAL (
            SELECT c.salary, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(dept_name, TRUE) OVER (ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS event_seq,
        LEAD(salary, 1) OVER (PARTITION BY location, dept_name ORDER BY employee_hire_date, id) AS next_salary
    FROM with_last
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        next_salary
    FROM with_windows
) src
PIVOT (
    AVG(next_salary)
    FOR dept_name IN ('Legal' AS legal, 'Marketing' AS marketing, 'HR' AS hr)
);

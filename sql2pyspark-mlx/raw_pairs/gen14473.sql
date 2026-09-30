WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(budget AS INT) AS budget_int
    FROM departments
    WHERE budget < 3086000
      AND dept_name IS NOT DISTINCT FROM 'HR'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, budget * budget_int AS budget_times_budget_int
    FROM filtered
    WHERE dept_name IN ('Engineering', 'Sales', 'Legal')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary < 65000
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_hire_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(budget_int, 2) OVER (ORDER BY budget_times_budget_int ASC, id ASC) AS prev_budget_int,
        NTILE(5) OVER (PARTITION BY location ORDER BY budget_int ASC, id ASC) AS budget_int_bucket,
        SUM(budget_int) OVER (ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_int
    FROM latest_match
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_name,
            location,
            budget_times_budget_int
        FROM scored
    ) src
    PIVOT (
        MAX(budget_times_budget_int)
        FOR location IN ('Berlin', 'Singapore')
    )
)
SELECT
    dept_name,
    Berlin,
    Singapore
FROM pivoted
ORDER BY Singapore DESC;

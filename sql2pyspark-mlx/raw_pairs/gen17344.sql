WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM departments
    WHERE dept_name NOT IN ('Marketing', 'HR')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(budget) OVER w_recent AS running_budget,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget NULLS FIRST) AS budget_cume_dist,
        LAST_VALUE(dept_name, TRUE) OVER w_recent AS last_known_dept_name
    FROM latest
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.running_budget,
        x.employee_salary,
        x.job_title
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary <= 115000
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary < 85000
        )
)
-- Running and moving aggregates
SELECT
    id,
    budget,
    running_budget,
    employee_salary,
    job_title,
    LAG(running_budget) OVER (PARTITION BY job_title ORDER BY budget, id) AS prev_running_budget,
    LEAD(running_budget) OVER (PARTITION BY job_title ORDER BY budget, id) AS next_running_budget,
    LAG(budget) OVER (PARTITION BY job_title ORDER BY budget, id) AS prev_budget,
    LEAD(budget) OVER (PARTITION BY job_title ORDER BY budget, id) AS next_budget,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY budget, id) AS seq_no,
    SUM(employee_salary) OVER (PARTITION BY job_title ORDER BY running_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_employee_salary
FROM eligible
WHERE job_title <> 'Manager'

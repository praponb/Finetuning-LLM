WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(budget * 1.07, 2) AS budget_scaled
    FROM departments
    WHERE dept_name <> 'Sales'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IS NULL
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        top1.employee_job_title,
        top1.employee_salary
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(location, 'Unknown') AS location_filled,
        employee_salary - budget AS employee_salary_minus_budget,
        COALESCE(employee_job_title, 'OTHER') AS employee_job_title_filled
    FROM with_latest
    WHERE employee_job_title IS NOT NULL
      AND employee_salary >= 140000
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY budget DESC NULLS LAST, id) AS row_num
    FROM valid_departments
    QUALIFY row_num = 1
)
SELECT
    id,
    location,
    employee_salary,
    location_filled,
    employee_job_title_filled,
    row_num
FROM latest
ORDER BY id DESC
LIMIT 5

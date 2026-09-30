WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(location, 1, 4) AS location_prefix
    FROM departments
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'Tokyo'
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_salary,
        top1.hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.employee_salary,
        latest.employee_hire_date,
        latest.employee_level
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote = TRUE
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN employee_salary >= 149000 THEN 'LARGE' ELSE 'SMALL' END AS employee_salary_tier,
    DATE_TRUNC('MONTH', employee_hire_date) AS activity_month,
    COUNT_IF(employee_level = 'L5') AS l5_events,
    COUNT(*) AS cnt
FROM eligible
GROUP BY employee_salary_tier, activity_month
HAVING COUNT(*) > 1
ORDER BY cnt DESC NULLS LAST

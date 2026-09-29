WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'Unknown') AS location_filled
    FROM departments
    WHERE dept_name IN ('Finance', 'Sales')
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget_int,
        x.level
    FROM valid s
    JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.hire_date,
        latest.job_title
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_level,
        top1.salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    CASE WHEN budget_int >= 87 THEN 'HIGH' ELSE 'LOW' END AS budget_int_tier,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    ROUND(SUM(budget_int) / NULLIF(COUNT(CASE WHEN level = 'L4' THEN 1 END), 0), 2) AS budget_int_per_l4,
    ROUND(SUM(budget_int) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS budget_int_per_l1,
    COUNT(DISTINCT employee_level) AS distinct_employee_levels
FROM with_last_departments
GROUP BY 1, 2, 3;

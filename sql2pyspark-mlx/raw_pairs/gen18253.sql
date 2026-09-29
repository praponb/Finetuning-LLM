WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(dept_name, ' ', location) AS dept_name_location,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE budget <= 1809000
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        location,
        budget,
        dept_name_location,
        dept_name_clean,
        TRIM(dept_name) AS clean_dept_name,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM base
    WHERE location = 'Singapore'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_hire_date,
        latest.level
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget) OVER w_part AS last4_avg_budget,
        MAX(budget) OVER w_part AS last4_max_budget,
        MIN(budget) OVER w_part AS last4_min_budget,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY budget, id) AS seq_no
    FROM latest_match
    WINDOW w_part AS (PARTITION BY location, level ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name_location,
        s.dept_name_clean,
        s.dept_name_filled,
        s.employee_hire_date,
        s.level,
        s.last4_avg_budget,
        s.last4_max_budget,
        s.last4_min_budget,
        top1.salary,
        top1.employee_job_title
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.employee_hire_date
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    level,
    COUNT(*) FILTER (WHERE level = 'L5') AS l5_rows,
    SUM(CASE WHEN level = 'L2' THEN last4_avg_budget ELSE 0 END) AS l2_last4_avg_budget,
    ROUND(AVG(salary), 2) AS avg_salary
FROM latest_match_departments
GROUP BY level
ORDER BY l5_rows DESC NULLS FIRST

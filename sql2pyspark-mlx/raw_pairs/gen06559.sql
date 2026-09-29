WITH latest AS (
    -- QUALIFY-style window filter
    SELECT id, dept_name
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.hire_date,
        x.employee_job_title
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_departments AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_name,
        employee_job_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title, dept_name ORDER BY hire_date DESC NULLS FIRST, id) AS latest_rank
        FROM with_latest
    ) ranked
    WHERE latest_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM latest_departments
    WHERE employee_job_title IN ('Analyst', 'Designer', 'Engineer')
)
-- Multi-level aggregation
SELECT
    dept_name,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
    SIZE(COLLECT_SET(employee_job_title)) AS num_employee_job_titles
FROM base
GROUP BY dept_name
ORDER BY num_employee_job_titles DESC NULLS LAST;

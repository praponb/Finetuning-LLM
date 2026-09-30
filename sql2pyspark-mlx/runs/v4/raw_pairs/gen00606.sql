WITH with_dims AS (
    -- Attach departments attributes
    SELECT s.*, d.location
    FROM employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        CAST(is_remote AS INT) AS is_remote_flag,
        ROUND(salary * 1.15, 2) AS adjusted_salary
    FROM with_dims
    WHERE hire_date >= TO_DATE('2024-02-19')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        s.*,
        p.budget,
        p.location
    FROM latest s
    INNER JOIN departments p
        ON s.dept_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    dept_id,
    COUNT(*) FILTER (WHERE job_title = 'Intern') AS intern_rows,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN location = 'Berlin' THEN 1 END), 0), 2) AS salary_per_berlin,
    ROUND(AVG(is_remote_flag), 2) AS avg_is_remote_flag
FROM with_dims_employees
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', hire_date), dept_id), (DATE_TRUNC('MONTH', hire_date)))
HAVING ROUND(AVG(is_remote_flag), 2) > 0;

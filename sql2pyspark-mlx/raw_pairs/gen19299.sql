WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(job_title)) AS job_title_title
    FROM employees
    WHERE hire_date > DATE '2023-04-08'
      AND level IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        job_title_title,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS seq_no,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share_2
    FROM filtered
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.level AS employee_level,
        p.salary AS employee_salary,
        p.email
    FROM with_windows src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM with_dims
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE level IN ('L5', 'L2', 'L4')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN kept_keys k ON s.manager_id = k.manager_id
)
-- Join a dimension and aggregate
SELECT
    p.level AS employee_level,
    p.job_title,
    a.employee_level AS employee_employee_level,
    COUNT(*) AS num_rows,
    AVG(a.salary_share_2) AS avg_salary_share_2,
    GROUPING_ID() AS grouping_level
FROM verified a
JOIN employees p ON a.manager_id = p.id
GROUP BY ROLLUP (p.level, p.job_title, a.employee_level)
ORDER BY num_rows NULLS FIRST

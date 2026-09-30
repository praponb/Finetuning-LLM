WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(hire_date) AS hire_date_month_end
    FROM employees
    WHERE hire_date >= DATE '2023-03-19'
      AND level IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    level,
    ROW_NUMBER() OVER (ORDER BY hire_date_month_end, id) AS seq_no,
    CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary) AS salary_cume_dist,
    ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share
FROM base
WHERE manager_id IS NULL;

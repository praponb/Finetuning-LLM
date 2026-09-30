WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(level) AS level_length
    FROM employees
    WHERE salary BETWEEN 57000 AND 118000
      AND manager_id IS NOT NULL
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.salary AS employee_salary,
        d.email AS employee_email
    FROM cleaned s
    JOIN employees d
        ON s.manager_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT manager_id, level
    FROM joined
    EXCEPT DISTINCT
    SELECT manager_id, level
    FROM employees
    WHERE email LIKE '%@gmail.com'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.manager_id = k.manager_id AND s.level = k.level
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        full_name,
        dept_id,
        job_title,
        hire_date,
        level,
        level_length,
        employee_email,
        RANK() OVER (ORDER BY level_length DESC) AS level_length_rank,
        AVG(salary) OVER (PARTITION BY MONTH(hire_date), dept_id) AS hire_date_month_avg_salary,
        MAX(salary) OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date), manager_id) AS hire_date_period_max_salary
    FROM kept_rows
)
SELECT
    full_name,
    dept_id,
    job_title,
    level,
    level_length,
    employee_email,
    level_length_rank,
    hire_date_period_max_salary,
    CASE WHEN level RLIKE '^(L4|L3)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS level_zone
FROM metrics

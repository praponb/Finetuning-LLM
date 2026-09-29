WITH matched_keys AS (
    -- Set difference on keys
    SELECT level, dept_id
    FROM employees
    EXCEPT
    SELECT level, dept_id
    FROM employees
    WHERE level <> 'L3'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    JOIN matched_keys k ON s.level = k.level AND s.dept_id = k.dept_id
)
-- Multi-level aggregation
SELECT
    job_title,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) AS salary_per_l5,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN job_title = 'Intern' THEN 1 END), 0), 2) AS salary_per_intern,
    STDDEV_SAMP(salary) AS salary_stddev
FROM verified
GROUP BY job_title, DATE_TRUNC('MONTH', hire_date);

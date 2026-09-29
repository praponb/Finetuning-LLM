WITH joined AS (
    -- Attach departments attributes
    SELECT src.*, b.location
    FROM employees src
    LEFT JOIN departments b
        ON src.dept_id = b.id
    WHERE b.budget BETWEEN 2607000 AND 3174000
),
with_dims AS (
    -- Attach employees attributes
    SELECT s.*, e.job_title AS employee_job_title
    FROM joined s
    LEFT JOIN employees e
        ON s.manager_id <=> e.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary, 2) OVER (ORDER BY salary, id) AS next_salary,
        NTILE(4) OVER (PARTITION BY location, level ORDER BY salary DESC, id) AS salary_bucket
    FROM with_dims
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.email,
        src.dept_id,
        src.hire_date,
        src.location,
        src.employee_job_title,
        src.salary_bucket,
        d.location AS department_location,
        d.budget
    FROM enriched src
    LEFT JOIN departments d
        ON src.dept_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary_bucket) OVER (ORDER BY salary_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_bucket_avg_5_prior,
        SUM(salary_bucket) OVER (PARTITION BY YEAR(hire_date), location ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_bucket
    FROM with_dims_employees
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(*) FILTER (WHERE employee_job_title = 'Analyst') AS analyst_rows,
    APPROX_PERCENTILE(salary_bucket_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS salary_bucket_avg_5_prior_quartiles,
    ROUND(SUM(salary_bucket_avg_5_prior) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS salary_bucket_avg_5_prior_per_tokyo,
    COUNT_IF(employee_job_title = 'Designer') AS designer_events,
    SORT_ARRAY(COLLECT_SET(employee_job_title)) AS employee_job_title_list,
    GROUPING(DATE_TRUNC('YEAR', hire_date)) AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('MONTH', hire_date)), (DATE_TRUNC('YEAR', hire_date)))
HAVING ROUND(SUM(salary_bucket_avg_5_prior) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) < 1000000
